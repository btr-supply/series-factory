use crate::types::{AggregationMode, Config, DataSource, GenerativeModel, WeightMode, normalize_weights};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use regex::Regex;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "BTC")]
    pub base: String,

    #[arg(long, default_value = "USDT")]
    pub quote: String,

    #[arg(long, default_value = "binance", value_delimiter = '|')]
    pub sources: Vec<String>,

    #[arg(long, default_value = "30-days-ago")]
    pub from: String,

    #[arg(long, default_value = "yesterday")]
    pub to: String,

    #[arg(long, value_enum, default_value = "time")]
    pub agg_mode: String,

    #[arg(long, default_value = "1000")]
    pub agg_step: f64,

    #[arg(long, default_value = "all", value_delimiter = '|')]
    pub agg_fields: Vec<String>,

    #[arg(long, default_value = "static")]
    pub weight_mode: String,

    #[arg(long, value_delimiter = '|')]
    pub weights: Option<Vec<f64>>,

    #[arg(long, default_value = "5000")]
    pub tick_ttl: i64,

    #[arg(long, default_value = "0.05")]
    pub tick_max_deviation: f64,

    #[arg(long, default_value = "parquet")]
    pub out_format: String,

    #[arg(long, default_value = "./cache")]
    pub cache_dir: PathBuf,

    #[arg(long, default_value = "./output")]
    pub output_dir: PathBuf,
}

impl Args {
    pub fn into_config(self) -> Result<Config> {
        let from = parse_datetime(&self.from)?;
        let to = parse_datetime(&self.to)?;

        let agg_mode = match self.agg_mode.as_str() {
            "tick" => AggregationMode::Tick,
            "time" => AggregationMode::Time,
            _ => return Err(anyhow!("Invalid aggregation mode")),
        };

        let weight_mode = match self.weight_mode.as_str() {
            "static" => WeightMode::Static,
            "volume" => WeightMode::Volume,
            "mixed" => WeightMode::Mixed,
            _ => return Err(anyhow!("Invalid weight mode")),
        };

        let weights = if let Some(w) = self.weights {
            w
        } else {
            vec![]  // Empty means equal weights
        };

        // Normalize weights to sum to 1.0
        let source_weights = normalize_weights(&weights, self.sources.len());

        // Handle "all" fields
        let agg_fields = if self.agg_fields.len() == 1 && self.agg_fields[0] == "all" {
            ["timestamp", "mid", "spread", "vbid", "vask", "velocity", "dispersion", "drift"]
            .map(String::from)
            .into()
        } else {
            self.agg_fields
        };

        Ok(Config {
            base: self.base,
            quote: self.quote,
            sources: self.sources,
            from,
            to,
            agg_mode,
            agg_step: self.agg_step,
            agg_fields,
            weight_mode,
            weights,
            source_weights,
            tick_ttl: self.tick_ttl,
            tick_max_deviation: self.tick_max_deviation,
            out_format: self.out_format,
            cache_dir: self.cache_dir,
            output_dir: self.output_dir,
        })
    }
}

fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    use chrono::{Duration, NaiveTime};
    
    let now = Utc::now();
    
    match s {
        "now" | "today" => Ok(now.date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "yesterday" => Ok((now - Duration::days(1)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "7-days-ago" => Ok((now - Duration::days(7)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "30-days-ago" => Ok((now - Duration::days(30)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        "90-days-ago" => Ok((now - Duration::days(90)).date_naive().and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc()),
        _ => {
            // Try to parse as regular date
            Ok(DateTime::parse_from_str(&format!("{} 00:00:00 +0000", s), "%Y-%m-%d %H:%M:%S %z")?
                .with_timezone(&Utc))
        }
    }
}

pub fn parse_data_source(source: &str) -> Result<DataSource> {
    // Check if it's a generative model
    let gbm_re = Regex::new(r"^gbm\(([^,]+),([^,]+),([^)]+)\)$")?;
    let fbm_re = Regex::new(r"^fbm\(([^,]+),([^,]+),([^,]+),([^)]+)\)$")?;
    let hm_re = Regex::new(r"^hm\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^)]+)\)$")?;
    let njdm_re = Regex::new(r"^njdm\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^)]+)\)$")?;
    let dejdm_re = Regex::new(r"^dejdm\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^)]+)\)$")?;

    if let Some(caps) = gbm_re.captures(source) {
        Ok(DataSource::Synthetic(GenerativeModel::GBM {
            mu: caps[1].parse()?,
            sigma: caps[2].parse()?,
            base: caps[3].parse()?,
        }))
    } else if let Some(caps) = fbm_re.captures(source) {
        Ok(DataSource::Synthetic(GenerativeModel::FBM {
            mu: caps[1].parse()?,
            sigma: caps[2].parse()?,
            hurst: caps[3].parse()?,
            base: caps[4].parse()?,
        }))
    } else if let Some(caps) = hm_re.captures(source) {
        Ok(DataSource::Synthetic(GenerativeModel::Heston {
            mu: caps[1].parse()?,
            sigma: caps[2].parse()?,
            kappa: caps[3].parse()?,
            theta: caps[4].parse()?,
            xi: caps[5].parse()?,
            rho: caps[6].parse()?,
            base: caps[7].parse()?,
        }))
    } else if let Some(caps) = njdm_re.captures(source) {
        Ok(DataSource::Synthetic(GenerativeModel::NormalJumpDiffusion {
            mu: caps[1].parse()?,
            sigma: caps[2].parse()?,
            lambda: caps[3].parse()?,
            mu_jump: caps[4].parse()?,
            sigma_jump: caps[5].parse()?,
            base: caps[6].parse()?,
        }))
    } else if let Some(caps) = dejdm_re.captures(source) {
        Ok(DataSource::Synthetic(GenerativeModel::DoubleExpJumpDiffusion {
            mu: caps[1].parse()?,
            sigma: caps[2].parse()?,
            lambda: caps[3].parse()?,
            mu_pos_jump: caps[4].parse()?,
            mu_neg_jump: caps[5].parse()?,
            p_neg_jump: caps[6].parse()?,
            base: caps[7].parse()?,
        }))
    } else {
        // It's an exchange name
        Ok(DataSource::Exchange(source.to_string()))
    }
}
