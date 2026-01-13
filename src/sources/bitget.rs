use crate::types::{Config, Tick, round_to_6_sig_digits};
use crate::sources::TickSource;
use anyhow::Result;
use chrono::Duration;
use csv::ReaderBuilder;
use rayon::prelude::*;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use std::fs;

pub struct BitgetSource {
    agent: Arc<ureq::Agent>,
}

impl BitgetSource {
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

    async fn get_data_files(&self, config: &Config) -> Result<Vec<PathBuf>> {
        let symbol = self.get_symbol(config);
        let mut files = Vec::new();

        let mut current_date = config.from.date_naive();
        let end_date = config.to.date_naive();

        while current_date <= end_date {
            let date_str = current_date.format("%Y%m%d").to_string();

            // Bitget splits daily data into multiple files (001, 002, 003, ...)
            // Try to fetch all files for this day
            let mut seq = 1;
            loop {
                let seq_str = format!("{:03}", seq);
                let filename = format!("{}_{}.zip", date_str, seq_str);
                let cache_path = config.cache_dir
                    .join("bitget")
                    .join(&symbol)
                    .join(filename.replace(".zip", ".parquet"));

                let url = format!(
                    "https://img.bitgetimg.com/online/trades/SPBL/{}/{}",
                    symbol, filename
                );

                // Try to download
                match self.download_and_convert_to_parquet(&url, &cache_path).await {
                    Ok(_) => {
                        files.push(cache_path);
                        seq += 1;
                    }
                    Err(_) => {
                        // No more files for this day
                        if seq == 1 {
                            debug!("No Bitget data found for {}", date_str);
                        }
                        break;
                    }
                }
            }

            current_date = current_date + Duration::days(1);
        }

        Ok(files)
    }

    async fn download_and_convert_to_parquet(&self, url: &str, cache_path: &Path) -> Result<()> {
        use arrow::array::{Int64Array, Float64Array, UInt32Array};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc as StdArc;

        // Create cache directory
        if let Some(parent) = cache_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Download ZIP data
        let response = self.agent.get(url).call()
            .map_err(|e| anyhow::anyhow!("Failed to download {}: {}", url, e))?;

        let mut zip_data = Vec::new();
        response.into_reader().read_to_end(&mut zip_data)?;

        // Extract CSV from ZIP
        let mut reader = std::io::Cursor::new(zip_data);
        let mut archive = zip::ZipArchive::new(&mut reader)?;

        let mut csv_data = Vec::new();
        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            if file.name().ends_with(".csv") {
                file.read_to_end(&mut csv_data)?;
                break;
            }
        }

        let ticks = Self::process_csv_data(&csv_data)?;

        // Convert to Arrow arrays
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

        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("bid", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("ask", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("vbid", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new("vask", arrow::datatypes::DataType::UInt32, false),
        ]);

        let batch = RecordBatch::try_new(
            StdArc::new(schema.clone()),
            vec![
                StdArc::new(Int64Array::from(timestamps)) as _,
                StdArc::new(Float64Array::from(bids)) as _,
                StdArc::new(Float64Array::from(asks)) as _,
                StdArc::new(UInt32Array::from(vbids)) as _,
                StdArc::new(UInt32Array::from(vasks)) as _,
            ],
        )?;

        let file = fs::File::create(cache_path)?;
        let mut writer = ArrowWriter::try_new(file, StdArc::new(schema), None)?;
        writer.write(&batch)?;
        writer.close()?;

        debug!("Converted to parquet: {}", cache_path.display());
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
            let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let bids = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            let asks = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            let vbids = batch.column(3).as_any().downcast_ref::<UInt32Array>().unwrap();
            let vasks = batch.column(4).as_any().downcast_ref::<UInt32Array>().unwrap();

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

    fn process_csv_data(csv_data: &[u8]) -> Result<Vec<Tick>> {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(Cursor::new(csv_data));

        let mut ticks = Vec::new();
        let mut is_first_tick = true;

        for result in reader.records() {
            let record = result?;

            // Bitget format: trade_id,timestamp,price,side,volume(quote),size(base)
            let timestamp: i64 = record[1].parse()?;
            let price: f64 = record[2].parse()?;
            let side = &record[3];
            let volume_quote: f64 = record[4].parse().unwrap_or(0.0);

            if is_first_tick {
                let p = round_to_6_sig_digits(price);
                ticks.push(Tick {
                    timestamp,
                    bid: p,
                    ask: p,
                    vbid: 0,
                    vask: 0,
                });
                is_first_tick = false;
            }

            // side: "buy" = market buy (taker buys), "sell" = market sell (taker sells)
            let tick = if side.eq_ignore_ascii_case("buy") {
                Tick {
                    timestamp,
                    bid: round_to_6_sig_digits(price * 0.9999),
                    ask: round_to_6_sig_digits(price),
                    vbid: 0,
                    vask: volume_quote as u32,
                }
            } else {
                Tick {
                    timestamp,
                    bid: round_to_6_sig_digits(price),
                    ask: round_to_6_sig_digits(price * 1.0001),
                    vbid: volume_quote as u32,
                    vask: 0,
                }
            };

            ticks.push(tick);
        }

        Ok(ticks)
    }
}

#[async_trait::async_trait]
impl TickSource for BitgetSource {
    async fn fetch_ticks(&self, config: &Config, tx: mpsc::Sender<Vec<Tick>>) -> Result<()> {
        info!("Fetching Bitget data for {}{}", config.base, config.quote);

        let files = self.get_data_files(config).await?;
        info!("Processing {} data files", files.len());

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

        for tick_result in all_ticks_results {
            match tick_result {
                Ok(ticks) => {
                    for batch in ticks.chunks(10000) {
                        if let Err(e) = tx.send(batch.to_vec()).await {
                            warn!("Failed to send tick batch: {}", e);
                            break;
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(())
    }
}
