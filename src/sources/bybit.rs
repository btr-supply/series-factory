use crate::types::{Config, Tick, round_to_6_sig_digits};
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
use std::fs;

pub struct BybitSource {
    agent: Arc<ureq::Agent>,
}

impl BybitSource {
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
            .join("bybit")
            .join(&self.get_symbol(config))
            .join(filename.replace(".csv.gz", ".parquet"))
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
                // Try monthly data first (format: BTCUSDT-2025-12.csv.gz)
                let filename = format!("{}-{:04}-{:02}.csv.gz", symbol, year, month);
                let cache_path = config.cache_dir
                    .join("bybit")
                    .join(&symbol)
                    .join(filename.replace(".csv.gz", ".parquet"));

                if !cache_path.exists() {
                    let url = format!(
                        "https://public.bybit.com/spot/{}/{}",
                        symbol, filename
                    );
                    info!("Downloading and converting monthly data: {}", filename);
                    if let Err(e) = self.download_and_convert_to_parquet(&url, &cache_path, &filename).await {
                        warn!("Failed to download monthly data {}: {}, trying daily files", filename, e);
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
        use flate2::read::GzDecoder;

        // Create temporary directory for download
        let temp_dir = std::env::temp_dir().join("series_factory");
        fs::create_dir_all(&temp_dir)?;
        let temp_gz = temp_dir.join(filename);

        // Download GZ file
        self.download_file(url, &temp_gz).await?;

        // Read and decompress
        let gz_data = fs::read(&temp_gz)?;
        let mut decoder = GzDecoder::new(Cursor::new(gz_data));
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;

        let ticks = Self::process_csv_data_parallel(&decompressed)?;

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

        // Clean up temporary GZ file
        let _ = fs::remove_file(temp_gz);

        info!("Converted {} to parquet: {}", filename, cache_path.display());
        Ok(())
    }

    fn read_parquet_file(file_path: &Path) -> Result<Vec<Tick>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use arrow::array::{Int64Array, Float64Array, UInt32Array};

        let file = fs::File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut ticks = Vec::new();

        for batch_result in reader {
            let batch = batch_result?;

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

            for i in 0..batch.num_rows() {
                ticks.push(Tick {
                    timestamp: timestamps.value(i),
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

        let start_date = month_start;
        let end_date = std::cmp::min(last_day_of_month, config.to.date_naive());

        let mut current_date = start_date;
        while current_date <= end_date {
            // Daily format: BTCUSDT_2025-01-01.csv.gz
            let date = current_date;
            let filename = format!("{}_{:04}-{:02}-{:02}.csv.gz", symbol, year, month, date.day());
            let cache_path = config.cache_dir
                .join("bybit")
                .join(&symbol)
                .join(filename.replace(".csv.gz", ".parquet"));

            if !cache_path.exists() {
                let url = format!(
                    "https://public.bybit.com/spot/{}/{}",
                    symbol, filename
                );
                info!("Downloading and converting daily data: {}", filename);
                if let Err(e) = self.download_and_convert_to_parquet(&url, &cache_path, &filename).await {
                    warn!("Failed to download daily data {}: {}", filename, e);
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

    /// Process CSV data using Rayon for parallel processing
    fn process_csv_data_parallel(csv_data: &[u8]) -> Result<Vec<Tick>> {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(Cursor::new(csv_data));

        let mut records = Vec::new();
        for result in reader.records() {
            let record = result?;
            records.push(record);
        }

        // Process in parallel using Rayon
        let tick_results: Result<Vec<Tick>, anyhow::Error> = records
            .par_iter()
            .map(|record| Self::process_record(record))
            .collect();

        let mut ticks = tick_results?;

        // Sort by timestamp
        ticks.par_sort_unstable_by_key(|t| t.timestamp);

        Ok(ticks)
    }

    /// Process a single CSV record into a Tick
    fn process_record(record: &csv::StringRecord) -> Result<Tick> {
        // Bybit format: id,timestamp,price,volume,side,[rpi]
        // Columns: 0=id, 1=timestamp, 2=price, 3=volume, 4=side, 5=rpi (optional)
        let timestamp: i64 = record[1].parse()?;
        let price: f64 = record[2].parse()?;
        let volume: f64 = record[3].parse()?;
        let side = &record[4];

        // Bybit side: "buy" = market buy (taker is buyer), "sell" = market sell (taker is seller)
        // This is opposite of Binance's is_buyer_maker
        // - Binance: is_buyer_maker=true means limit order was bid (market sell)
        // - Bybit: side="buy" means market buy (taker buys)

        let is_market_buy = side.eq_ignore_ascii_case("buy");

        // Convert to tick format
        // Market sell (taker sells) -> price becomes bid
        // Market buy (taker buys) -> price becomes ask
        let tick = if is_market_buy {
            Tick {
                timestamp,
                bid: round_to_6_sig_digits(price * 0.9999), // Estimate bid
                ask: round_to_6_sig_digits(price),           // Price is ask
                vbid: 0,
                vask: (volume * price) as u32,
            }
        } else {
            Tick {
                timestamp,
                bid: round_to_6_sig_digits(price),           // Price is bid
                ask: round_to_6_sig_digits(price * 1.0001), // Estimate ask
                vbid: (volume * price) as u32,
                vask: 0,
            }
        };

        Ok(tick)
    }
}

#[async_trait::async_trait]
impl TickSource for BybitSource {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<Tick>>,
    ) -> Result<()> {
        info!("Fetching Bybit data for {}{}", config.base, config.quote);

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
                    continue;
                }
            }
        }

        Ok(())
    }
}
