use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Round a float to 6 significant digits
pub fn round_to_6_sig_digits(value: f64) -> f64 {
    if value == 0.0 || !value.is_finite() {
        return value;
    }
    let magnitude = value.abs().log10().floor();
    let factor = 10.0_f64.powi(5 - magnitude as i32);
    (value * factor).round() / factor
}

/// A single market tick with bid/ask prices and volumes
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tick {
    pub timestamp: i64,   // milliseconds since epoch
    pub bid: f64,
    pub ask: f64,
    pub vbid: u32,        // volume at bid in USD*
    pub vask: u32,        // volume at ask in USD*
}

impl PartialOrd for Tick {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl Ord for Tick {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl Eq for Tick {}

/// Aggregated OHLCV data for a time or price bucket
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Aggregate {
    pub timestamp: i64,     // milliseconds since epoch
    pub open: f64,         // opening price (first tick mid in bucket)
    pub high: f64,         // highest price in bucket
    pub low: f64,          // lowest price in bucket
    pub close: f64,        // closing price (last tick mid in bucket)
    pub mid: f64,          // alias for close (standard bid|ask|mid format)
    pub spread: f32,       // average spread as ratio
    pub vbid: u32,         // cumulative volume at bid
    pub vask: u32,         // cumulative volume at ask
    pub velocity: f32,     // sqrt of tick count (activity measure)
    pub dispersion: f32,   // normalized standard deviation (volatility measure)
    pub drift: f32,        // normalized linear regression slope (trend measure)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregationMode {
    Tick,
    Time,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WeightMode {
    Static,
    Volume,
    Mixed,
}

impl std::fmt::Display for AggregationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationMode::Tick => write!(f, "tick"),
            AggregationMode::Time => write!(f, "time"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base: String,
    pub quote: String,
    pub sources: Vec<String>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub agg_mode: AggregationMode,
    pub agg_step: f64,
    #[allow(dead_code)]
    pub agg_fields: Vec<String>,
    pub weight_mode: WeightMode,
    pub weights: Vec<f64>,
    /// Normalized weights for each source (sums to 1.0)
    pub source_weights: Vec<f64>,
    #[allow(dead_code)]
    pub tick_ttl: i64, // milliseconds
    pub tick_max_deviation: f64,
    #[allow(dead_code)]
    pub out_format: String,
    pub cache_dir: PathBuf,
    pub output_dir: PathBuf,
}

/// Normalize weights so they sum to 1.0. If empty, returns equal weights for n sources.
pub fn normalize_weights(weights: &[f64], n_sources: usize) -> Vec<f64> {
    if weights.is_empty() {
        // Equal weights
        vec![1.0 / n_sources as f64; n_sources]
    } else if weights.len() != n_sources {
        // Mismatch: pad or truncate to match n_sources
        let mut result = weights.to_vec();
        while result.len() < n_sources {
            result.push(1.0);
        }
        result.truncate(n_sources);
        let sum: f64 = result.iter().sum();
        if sum > 0.0 {
            result.iter().map(|w| w / sum).collect()
        } else {
            vec![1.0 / n_sources as f64; n_sources]
        }
    } else {
        let sum: f64 = weights.iter().sum();
        if sum > 0.0 {
            weights.iter().map(|w| w / sum).collect()
        } else {
            vec![1.0 / n_sources as f64; n_sources]
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggTrade {
    #[allow(dead_code)]
    pub agg_trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    #[allow(dead_code)]
    pub first_trade_id: i64,
    #[allow(dead_code)]
    pub last_trade_id: i64,
    pub transact_time: i64,
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone)]
pub enum DataSource {
    Exchange(String),
    Synthetic(GenerativeModel),
}

#[derive(Debug, Clone)]
pub enum GenerativeModel {
    GBM { mu: f64, sigma: f64, base: f64 },
    FBM { mu: f64, sigma: f64, hurst: f64, base: f64 },
    Heston { 
        mu: f64, 
        sigma: f64, 
        kappa: f64, 
        theta: f64, 
        xi: f64, 
        rho: f64, 
        base: f64 
    },
    NormalJumpDiffusion {
        mu: f64,
        sigma: f64,
        lambda: f64,
        mu_jump: f64,
        sigma_jump: f64,
        base: f64,
    },
    DoubleExpJumpDiffusion {
        mu: f64,
        sigma: f64,
        lambda: f64,
        mu_pos_jump: f64,
        mu_neg_jump: f64,
        p_neg_jump: f64,
        base: f64,
    },
}