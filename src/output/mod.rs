use crate::cache::CacheManager;
use crate::types::{Aggregate, Config};
use anyhow::Result;
use arrow::array::{Float64Array, Float32Array, Int64Array, UInt32Array};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// Writes aggregated time series data to Parquet format
pub struct OutputWriter {
    _cache: CacheManager,
}

impl OutputWriter {
    /// Create a new output writer
    pub fn new(cache: CacheManager) -> Self {
        Self { _cache: cache }
    }

    /// Write aggregates to a Parquet file
    pub async fn write_aggregates(&self, config: &Config, aggregates: &[Aggregate]) -> Result<PathBuf> {
        if aggregates.is_empty() {
            anyhow::bail!("No aggregates to write");
        }

        // Create output directory
        std::fs::create_dir_all(&config.output_dir)?;

        // Generate filename
        let filename = self.generate_filename(config);
        let output_path = config.output_dir.join(&filename);

        // Convert aggregates to Arrow arrays
        let timestamps: Vec<i64> = aggregates.iter().map(|a| a.timestamp).collect();
        let opens: Vec<f64> = aggregates.iter().map(|a| a.open).collect();
        let highs: Vec<f64> = aggregates.iter().map(|a| a.high).collect();
        let lows: Vec<f64> = aggregates.iter().map(|a| a.low).collect();
        let closes: Vec<f64> = aggregates.iter().map(|a| a.close).collect();
        let mids: Vec<f64> = aggregates.iter().map(|a| a.mid).collect();
        let spreads: Vec<f32> = aggregates.iter().map(|a| a.spread).collect();
        let vbids: Vec<u32> = aggregates.iter().map(|a| a.vbid).collect();
        let vasks: Vec<u32> = aggregates.iter().map(|a| a.vask).collect();
        let velocities: Vec<f32> = aggregates.iter().map(|a| a.velocity).collect();
        let dispersions: Vec<f32> = aggregates.iter().map(|a| a.dispersion).collect();
        let drifts: Vec<f32> = aggregates.iter().map(|a| a.drift).collect();

        // Create Arrow arrays
        let timestamp_array = Int64Array::from(timestamps);
        let open_array = Float64Array::from(opens);
        let high_array = Float64Array::from(highs);
        let low_array = Float64Array::from(lows);
        let close_array = Float64Array::from(closes);
        let mid_array = Float64Array::from(mids);
        let spread_array = Float32Array::from(spreads);
        let vbid_array = UInt32Array::from(vbids);
        let vask_array = UInt32Array::from(vasks);
        let velocity_array = Float32Array::from(velocities);
        let dispersion_array = Float32Array::from(dispersions);
        let drift_array = Float32Array::from(drifts);

        // Create schema
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("open", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("high", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("low", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("close", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("mid", arrow::datatypes::DataType::Float64, false),
            arrow::datatypes::Field::new("spread", arrow::datatypes::DataType::Float32, false),
            arrow::datatypes::Field::new("vbid", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new("vask", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new("velocity", arrow::datatypes::DataType::Float32, false),
            arrow::datatypes::Field::new("dispersion", arrow::datatypes::DataType::Float32, false),
            arrow::datatypes::Field::new("drift", arrow::datatypes::DataType::Float32, false),
        ]);

        // Create record batch
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(timestamp_array) as _,
                Arc::new(open_array) as _,
                Arc::new(high_array) as _,
                Arc::new(low_array) as _,
                Arc::new(close_array) as _,
                Arc::new(mid_array) as _,
                Arc::new(spread_array) as _,
                Arc::new(vbid_array) as _,
                Arc::new(vask_array) as _,
                Arc::new(velocity_array) as _,
                Arc::new(dispersion_array) as _,
                Arc::new(drift_array) as _,
            ],
        )?;

        // Write to parquet file
        let file = File::create(&output_path)?;
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None)?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(output_path)
    }

    /// Generate output filename based on config
    fn generate_filename(&self, config: &Config) -> String {
        let from_date = config.from.format("%Y%m%d");
        let to_date = config.to.format("%Y%m%d");
        let sources = config.sources.join("|");
        let mode = config.agg_mode.to_string();
        let step = config.agg_step as u64;

        format!(
            "{}-{}_{}_{}-{}_{}-{}.parquet",
            config.base.to_lowercase(),
            config.quote.to_lowercase(),
            sources,
            from_date,
            to_date,
            mode,
            step
        )
    }
}
