use crate::types::{AggTrade, Config, Tick, round_to_6_sig_digits};
use crate::sources::TickSource;
use anyhow::Result;
use chrono::{Datelike, Duration, NaiveDate, Utc};
use csv::ReaderBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use zip::ZipArchive;
use std::fs;

pub struct BinanceSource {
    agent: Arc<ureq::Agent>,
}

impl BinanceSource {
    pub fn new() -> Self {
        Self {
            agent: Arc::new(
                ureq::AgentBuilder::new()
                    .timeout(std::time::Duration::from_secs(300))
                    .build()
            ),
        }
    }


    fn get_symbol(&self, config: &Config) -> String {
        format!("{}{}", config.base, config.quote)
    }

    fn get_cache_path(&self, config: &Config, filename: &str) -> PathBuf {
        config.cache_dir
            .join("binance")
            .join(&self.get_symbol(config))
            .join(filename.replace(".zip", ".parquet"))
    }


    async fn ensure_cache_dir(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    async fn download_file(&self, url: &str, path: &Path) -> Result<()> {
        self.ensure_cache_dir(path).await?;
        
        let agent = self.agent.clone();
        let url_copy = url.to_string();
        let path_copy = path.to_path_buf();
        
        // Use blocking thread pool for synchronous ureq
        tokio::task::spawn_blocking(move || -> Result<()> {
            debug!("Downloading: {}", url_copy);
            
            let response = agent.get(&url_copy).call()
                .map_err(|e| anyhow::anyhow!("Failed to download {}: {}", url_copy, e))?;
            
            let total_size = response.header("content-length")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            
            let pb = ProgressBar::new(total_size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                    .unwrap()
                    .progress_chars("#>-"),
            );

            let mut file = fs::File::create(&path_copy)
                .map_err(|e| anyhow::anyhow!("Failed to create file: {}", e))?;
            
            let mut reader = response.into_reader();
            let mut buffer = [0; 8192];
            let mut downloaded = 0;

            loop {
                let bytes_read = reader.read(&mut buffer)
                    .map_err(|e| anyhow::anyhow!("Failed to read response: {}", e))?;
                if bytes_read == 0 {
                    break;
                }
                file.write_all(&buffer[..bytes_read])
                    .map_err(|e| anyhow::anyhow!("Failed to write to file: {}", e))?;
                downloaded += bytes_read as u64;
                pb.set_position(downloaded);
            }

            pb.finish_with_message("Downloaded");
            Ok(())
        }).await?
    }

    async fn get_data_files(&self, config: &Config) -> Result<Vec<PathBuf>> {
        let symbol = self.get_symbol(config);
        let mut files = Vec::new();
        
        let mut current_date = config.from.date_naive();
        let end_date = config.to.date_naive();
        let today = Utc::now().date_naive();

        while current_date <= end_date {
            let year = current_date.year();
            let month = current_date.month();
            
            // Check if we should use monthly or daily data
            let last_day_of_month = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap() - Duration::days(1)
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap() - Duration::days(1)
            };

            if last_day_of_month < today {
                // Use monthly data
                let filename = format!("{}-aggTrades-{:04}-{:02}.zip", symbol, year, month);
                let cache_path = self.get_cache_path(config, &filename);
                
                if !cache_path.exists() {
                    let url = format!(
                        "https://data.binance.vision/data/spot/monthly/aggTrades/{}/{}",
                        symbol, filename
                    );
                    info!("Downloading and converting monthly data: {}", filename);
                    if let Err(e) = self.download_and_convert_to_parquet(&url, &cache_path, &filename).await {
                        warn!("Failed to download monthly data {}: {}", filename, e);
                        // Fall back to daily data for this month
                        self.download_daily_for_month(config, current_date, &mut files).await?;
                    } else {
                        files.push(cache_path);
                    }
                } else {
                    debug!("Using cached parquet file: {}", cache_path.display());
                    files.push(cache_path);
                }
                
                // Move to next month
                current_date = if month == 12 {
                    NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
                } else {
                    NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
                };
            } else {
                // Use daily data for current/future months
                self.download_daily_for_month(config, current_date, &mut files).await?;
                
                // Move to next month
                current_date = if month == 12 {
                    NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
                } else {
                    NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
                };
            }
        }

        Ok(files)
    }

    async fn download_and_convert_to_parquet(&self, url: &str, cache_path: &Path, filename: &str) -> Result<()> {
        use arrow::array::{Int64Array, Float64Array, UInt32Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc as StdArc;
        
        // Create temporary directory for ZIP download
        let temp_dir = std::env::temp_dir().join("series_factory");
        fs::create_dir_all(&temp_dir)?;
        let temp_zip = temp_dir.join(filename);
        
        // Download ZIP file to temporary location
        self.download_file(url, &temp_zip).await?;
        
        // Read and process the ZIP file
        let file_data = fs::read(&temp_zip)?;
        let ticks = Self::process_zip_data_parallel(&file_data)?;
        
        // Convert ticks to Arrow arrays
        let mut timestamps = Vec::with_capacity(ticks.len());
        let mut bids = Vec::with_capacity(ticks.len());
        let mut asks = Vec::with_capacity(ticks.len());
        let mut vbids = Vec::with_capacity(ticks.len());
        let mut vasks = Vec::with_capacity(ticks.len());
        
        for tick in ticks {
            timestamps.push(tick.timestamp);
            bids.push(tick.bid);
            asks.push(tick.ask);
            vbids.push(tick.vbid);
            vasks.push(tick.vask);
        }
        
        // Create Arrow arrays
        let timestamp_array = Int64Array::from(timestamps);
        let bid_array = Float64Array::from(bids);
        let ask_array = Float64Array::from(asks);
        let vbid_array = UInt32Array::from(vbids);
        let vask_array = UInt32Array::from(vasks);
        
        // Create schema
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("bid", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("ask", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("vbid", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new("vask", arrow::datatypes::DataType::UInt32, false),
        ]);
        
        // Create record batch
        let batch = RecordBatch::try_new(
            StdArc::new(schema.clone()),
            vec![
                StdArc::new(timestamp_array) as _,
                StdArc::new(bid_array) as _,
                StdArc::new(ask_array) as _,
                StdArc::new(vbid_array) as _,
                StdArc::new(vask_array) as _,
            ],
        )?;
        
        // Ensure cache directory exists
        self.ensure_cache_dir(cache_path).await?;
        
        // Write to parquet file
        let file = fs::File::create(cache_path)?;
        let mut writer = ArrowWriter::try_new(file, StdArc::new(schema), None)?;
        writer.write(&batch)?;
        writer.close()?;
        
        // Clean up temporary ZIP file
        let _ = fs::remove_file(temp_zip);
        
        info!("Converted {} to parquet: {}", filename, cache_path.display());
        Ok(())
    }

    fn read_parquet_file(file_path: &Path) -> Result<Vec<Tick>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use arrow::array::{Int64Array, Float64Array, UInt32Array};
        
        // Open parquet file
        let file = fs::File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        
        let mut ticks = Vec::new();
        
        // Read all batches
        for batch_result in reader {
            let batch = batch_result?;
            
            // Extract columns
            let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast timestamp column"))?;
            let bids = batch.column(1).as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast bid column"))?;
            let asks = batch.column(2).as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast ask column"))?;
            let vbids = batch.column(3).as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast vbid column"))?;
            let vasks = batch.column(4).as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to cast vask column"))?;
            
            // Convert to ticks
            for i in 0..batch.num_rows() {
                // Normalize timestamp units to milliseconds if needed
                let mut ts = timestamps.value(i);
                if ts > 10_i64.pow(13) { // likely microseconds
                    ts /= 1000;
                }
                ticks.push(Tick {
                    timestamp: ts,
                    bid: bids.value(i),
                    ask: asks.value(i),
                    vbid: vbids.value(i),
                    vask: vasks.value(i),
                });
            }
        }
        
        Ok(ticks)
    }

    async fn download_daily_for_month(
        &self,
        config: &Config,
        month_start: NaiveDate,
        files: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let symbol = self.get_symbol(config);
        let year = month_start.year();
        let month = month_start.month();
        
        let last_day_of_month = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap() - Duration::days(1)
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap() - Duration::days(1)
        };

        // Fixed: Compare full dates, not just day numbers
        let start_date = month_start;
        let end_date = std::cmp::min(last_day_of_month, config.to.date_naive());

        let mut current_date = start_date;
        while current_date <= end_date {
            let date = current_date;
            let filename = format!("{}-aggTrades-{}.zip", symbol, date.format("%Y-%m-%d"));
            let cache_path = self.get_cache_path(config, &filename);
            
            if !cache_path.exists() {
                let url = format!(
                    "https://data.binance.vision/data/spot/daily/aggTrades/{}/{}",
                    symbol, filename
                );
                info!("Downloading and converting daily data: {}", filename);
                if let Err(e) = self.download_and_convert_to_parquet(&url, &cache_path, &filename).await {
                    warn!("Failed to download daily data {}: {}", filename, e);
                    // Continue to next day if download failed
                    current_date = current_date + Duration::days(1);
                    continue;
                }
            } else {
                debug!("Using cached parquet file: {}", cache_path.display());
            }
            
            files.push(cache_path);
            
            current_date = current_date + Duration::days(1);
        }

        Ok(())
    }


    // Process ZIP data using Rayon for parallel CSV processing within the ZIP
    fn process_zip_data_parallel(file_data: &Vec<u8>) -> Result<Vec<Tick>> {
        let cursor = Cursor::new(file_data);
        let mut archive = ZipArchive::new(cursor)?;
        
        // Extract all CSV files from the ZIP sequentially (ZIP access isn't thread-safe)
        let mut csv_files = Vec::new();
        for i in 0..archive.len() {
            match archive.by_index(i) {
                Ok(mut file) => {
                    if !file.name().ends_with(".csv") {
                        continue;
                    }
                    
                    let mut csv_data = Vec::new();
                    if std::io::Read::read_to_end(&mut file, &mut csv_data).is_ok() {
                        csv_files.push(csv_data);
                    }
                }
                Err(_) => continue,
            }
        }

        // Process each CSV file in parallel using Rayon
        let tick_batches: Vec<Vec<Tick>> = csv_files
            .par_iter()
            .filter_map(|csv_data| {
                Self::process_csv_data(csv_data).ok()
            })
            .collect();

        // Combine all ticks and sort by timestamp
        let mut all_ticks: Vec<Tick> = tick_batches
            .into_iter()
            .flatten()
            .collect();
            
        // Sort ticks by timestamp using Rayon
        all_ticks.par_sort_unstable_by_key(|t| t.timestamp);
        
        Ok(all_ticks)
    }

    // Process individual CSV data (pure CPU work, no I/O)
    fn process_csv_data(csv_data: &[u8]) -> Result<Vec<Tick>> {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(Cursor::new(csv_data));
        
        let mut ticks = Vec::new();
        let mut last_bid = 0.0f64;
        let mut last_ask = 0.0f64;
        let mut is_first_tick = true;

        for result in reader.records() {
            let record = result?;
            
            let agg_trade = AggTrade {
                agg_trade_id: record[0].parse()?,
                price: record[1].parse()?,
                quantity: record[2].parse()?,
                first_trade_id: record[3].parse()?,
                last_trade_id: record[4].parse()?,
                transact_time: record[5].parse()?,
                is_buyer_maker: record[6].to_lowercase() == "true",
            };
            
            // Binance timestamps are already in milliseconds
            // Normalize timestamp to milliseconds (Binance may provide microseconds in some dumps)
            let mut timestamp_ms = agg_trade.transact_time;
            if timestamp_ms > 10_i64.pow(13) { // greater than ~Sat Nov 20 2286 in ms -> likely microseconds
                timestamp_ms /= 1000;
            }
            
            // For the first tick, initialize both bid and ask to the same price
            if is_first_tick {
                last_bid = round_to_6_sig_digits(agg_trade.price);
                last_ask = round_to_6_sig_digits(agg_trade.price);
                is_first_tick = false;
            }
            
            // Convert aggTrade to tick
            let tick = if agg_trade.is_buyer_maker {
                // Market sell - price is bid
                last_bid = round_to_6_sig_digits(agg_trade.price);
                let ask_price = round_to_6_sig_digits(last_ask.max(agg_trade.price * 1.0001));
                Tick {
                    timestamp: timestamp_ms,
                    bid: last_bid,
                    ask: ask_price, // Ensure ask > bid
                    vbid: (agg_trade.quantity * agg_trade.price) as u32,
                    vask: 0,
                }
            } else {
                // Market buy - price is ask
                last_ask = round_to_6_sig_digits(agg_trade.price);
                let bid_price = round_to_6_sig_digits(last_bid.min(agg_trade.price * 0.9999));
                Tick {
                    timestamp: timestamp_ms,
                    bid: bid_price, // Ensure bid < ask
                    ask: last_ask,
                    vbid: 0,
                    vask: (agg_trade.quantity * agg_trade.price) as u32,
                }
            };
            
            ticks.push(tick);
        }
        
        Ok(ticks)
    }

}

#[async_trait::async_trait]
impl TickSource for BinanceSource {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<Tick>>,
    ) -> Result<()> {
        info!("Fetching Binance data for {}{}", config.base, config.quote);
        
        let files = self.get_data_files(config).await?;
        info!("Processing {} data files", files.len());
        
        // Process parquet files in parallel using Rayon
        info!("Reading {} parquet files in parallel using Rayon thread pool", files.len());
        
        let all_ticks_results: Vec<Result<Vec<Tick>>> = files
            .par_iter()
            .map(|file_path| {
                Self::read_parquet_file(file_path)
                    .map_err(|e| {
                        warn!("Error processing parquet file {:?}: {}", file_path, e);
                        e
                    })
            })
            .collect();

        // Send all successfully processed ticks
        for tick_result in all_ticks_results {
            match tick_result {
                Ok(ticks) => {
                    // Send ticks in batches to avoid overwhelming the channel
                    for batch in ticks.chunks(10000) {
                        if let Err(e) = tx.send(batch.to_vec()).await {
                            warn!("Failed to send tick batch: {}", e);
                            break;
                        }
                    }
                }
                Err(_) => {
                    // Error already logged above
                    continue;
                }
            }
        }
        
        Ok(())
    }
}