use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// Constants
pub const DEFAULT_SPREAD: f32 = 0.0001;
pub const PARALLEL_THRESHOLD: usize = 1000;
pub const STREAMING_BUFFER_SIZE: usize = 100_000;

/// Round a float to 6 significant digits
#[inline]
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
    pub mid: f64,          // alias to close (for price aggregates/Renko-style ticks)
    pub spread: f32,       // average spread as ratio
    pub vbid: u32,         // cumulative volume at bid
    pub vask: u32,         // cumulative volume at ask
    pub velocity: f32,     // sqrt of tick count (activity measure)
    pub dispersion: f32,   // normalized standard deviation (volatility measure)
    pub drift: f32,        // normalized linear regression slope (trend measure)
    pub is_synthetic: bool, // true if generated to fill time gaps
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregationMode {
    Tick,
    Time,
}

impl std::fmt::Display for AggregationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationMode::Tick => write!(f, "tick"),
            AggregationMode::Time => write!(f, "time"),
        }
    }
}

/// How to weight data from multiple sources
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum WeightMode {
    #[default]
    Static,  // Use provided weights directly
    Volume,  // Weight by trading volume
    Mixed,   // Blend static and volume weights
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
    /// Fields to include in output (e.g., ["ohlc", "mid", "spread", "velocity"])
    /// Reserved for future selective field output
    #[allow(dead_code)]
    pub agg_fields: Vec<String>,
    /// How to weight multiple sources
    #[allow(dead_code)]
    pub weight_mode: WeightMode,
    /// Raw weights per source (before normalization)
    #[allow(dead_code)]
    pub weights: Vec<f64>,
    /// Normalized weights (sum to 1.0)
    #[allow(dead_code)]
    pub source_weights: Vec<f64>,
    pub tick_max_deviation: f64,
    pub cache_dir: PathBuf,
    pub output_dir: PathBuf,
}

/// Normalize weights to sum to 1.0. Returns equal weights if input is empty.
pub fn normalize_weights(weights: &[f64], n_sources: usize) -> Vec<f64> {
    if n_sources == 0 {
        return vec![];
    }
    if weights.is_empty() {
        return vec![1.0 / n_sources as f64; n_sources];
    }

    let mut result = weights.to_vec();
    result.resize(n_sources, 1.0); // Pad or truncate

    let sum: f64 = result.iter().sum();
    if sum > 0.0 {
        result.iter().map(|w| w / sum).collect()
    } else {
        vec![1.0 / n_sources as f64; n_sources]
    }
}

/// Aggregated trade from exchange API (Binance format)
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used for CSV parsing, not all read after
pub struct AggTrade {
    pub agg_trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub first_trade_id: i64,
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