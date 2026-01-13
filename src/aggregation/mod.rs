use crate::types::{Aggregate, AggregationMode, Config, Tick, round_to_6_sig_digits};
use rayon::prelude::*;

pub struct Aggregator {
    config: Config,
    current_bucket: Vec<Tick>,
    bucket_start_time: i64,
    last_aggregate_price: f64,
}

impl Aggregator {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            current_bucket: Vec::new(),
            bucket_start_time: -1,
            last_aggregate_price: 0.0,
        }
    }

    pub fn process_ticks(&mut self, ticks: &[Tick]) -> Vec<Aggregate> {
        let mut aggregates = Vec::new();

        for tick in ticks {
            match self.config.agg_mode {
                AggregationMode::Time => {
                    // Initialize bucket start aligned to step boundary
                    if self.bucket_start_time < 0 {
                        let step = self.config.agg_step as i64;
                        let aligned = if step > 0 { (tick.timestamp / step) * step } else { tick.timestamp };
                        self.bucket_start_time = aligned;
                    }

                    // If tick is beyond the current bucket window, flush current bucket
                    let step = self.config.agg_step as i64;
                    while tick.timestamp >= self.bucket_start_time + step {
                        // Always create an aggregate for each time bucket, even if empty
                        let agg = self.create_aggregate(self.bucket_start_time + step);
                        aggregates.push(agg);
                        self.current_bucket.clear();
                        
                        // Advance bucket by fixed step until tick fits
                        self.bucket_start_time += step;
                    }

                    // Accumulate tick into current bucket
                    self.current_bucket.push(*tick);
                }
                AggregationMode::Tick => {
                    // Price-based aggregation: accumulate until price moves by agg_step ratio
                    let mid = (tick.bid + tick.ask) / 2.0;
                    if self.last_aggregate_price == 0.0 {
                        self.last_aggregate_price = mid;
                    }
                    let step_ratio = self.config.agg_step; // e.g., 0.0005 for 5 bps
                    let upper = self.last_aggregate_price * (1.0 + step_ratio);
                    let lower = self.last_aggregate_price / (1.0 + step_ratio);

                    if !self.current_bucket.is_empty() && (mid >= upper || mid <= lower) {
                        let agg = self.create_aggregate(tick.timestamp);
                        if agg.close > 0.0 {  // Only add if valid
                            aggregates.push(agg);
                        }
                        self.current_bucket.clear();
                        // Reset reference price after closing bucket
                        self.last_aggregate_price = mid;
                    }

                    self.current_bucket.push(*tick);
                }
            }
        }

        aggregates
    }

    pub fn finalize(&mut self) -> Option<Aggregate> {
        if self.current_bucket.is_empty() && self.last_aggregate_price == 0.0 {
            return None;
        }

        let ts = match self.config.agg_mode {
            AggregationMode::Time => {
                if self.bucket_start_time >= 0 {
                    self.bucket_start_time + self.config.agg_step as i64
                } else {
                    self.current_bucket.last().map(|t| t.timestamp).unwrap_or(0)
                }
            }
            AggregationMode::Tick => self.current_bucket.last().map(|t| t.timestamp).unwrap_or(0),
        };

        Some(self.create_aggregate(ts))
    }

    // Bucket rollover handled directly in process_ticks for correctness

    fn create_aggregate(&mut self, aggregate_timestamp: i64) -> Aggregate {
        if self.current_bucket.is_empty() {
            // For empty buckets in time-based aggregation, carry forward the last known price
            // This creates a "flat" candle showing no activity
            if self.last_aggregate_price == 0.0 {
                // Return a placeholder that will be filtered out
                return Aggregate {
                    timestamp: aggregate_timestamp,
                    open: 0.0,
                    high: 0.0,
                    low: 0.0,
                    close: 0.0,
                    mid: 0.0,
                    spread: 0.0,
                    vbid: 0,
                    vask: 0,
                    velocity: 0.0,
                    dispersion: 0.0,
                    drift: 0.0,
                };
            }
            
            // Create a flat candle at last known price (no price movement during this period)
            return Aggregate {
                timestamp: aggregate_timestamp,
                open: self.last_aggregate_price,
                high: self.last_aggregate_price,
                low: self.last_aggregate_price,
                close: self.last_aggregate_price,
                mid: self.last_aggregate_price,
                spread: 0.0001, // Minimal spread for empty periods
                vbid: 0,
                vask: 0,
                velocity: 0.0,
                dispersion: 0.0,
                drift: 0.0,
            };
        }

        // Use all ticks in the current bucket
        let bucket_ticks: Vec<&Tick> = self.current_bucket.iter().collect();

        // Calculate OHLC from tick mid prices
        // For time-based: OHLC represents price movement within the time bucket
        // For price-based (Renko-like): OHLC shows the path taken to trigger the bucket close
        
        // Calculate initial close price for filtering
        let tick_mids: Vec<f64> = bucket_ticks.iter()
            .map(|t| (t.bid + t.ask) / 2.0)
            .collect();
        let close = *tick_mids.last().unwrap();  // Last tick mid price for filtering

        // Calculate spreads for filtering (not used in final output)
        let _spreads: Vec<f64> = if bucket_ticks.len() > 1000 {
            bucket_ticks
                .par_iter()
                .map(|tick| {
                    let mid = (tick.bid + tick.ask) / 2.0;
                    (tick.ask - tick.bid) / mid
                })
                .collect()
        } else {
            bucket_ticks
                .iter()
                .map(|tick| {
                    let mid = (tick.bid + tick.ask) / 2.0;
                    (tick.ask - tick.bid) / mid
                })
                .collect()
        };

        // Filter ticks by max deviation around the close price
        let valid_ticks: Vec<&Tick> = bucket_ticks
            .into_iter()
            .filter(|t| {
                let mid = (t.bid + t.ask) / 2.0;
                let deviation = (mid - close).abs() / close;
                deviation <= self.config.tick_max_deviation
            })
            .collect();

        if valid_ticks.is_empty() {
            // Return a placeholder with last known price
            return Aggregate {
                timestamp: aggregate_timestamp,
                open: self.last_aggregate_price,
                high: self.last_aggregate_price,
                low: self.last_aggregate_price,
                close: self.last_aggregate_price,
                mid: self.last_aggregate_price,
                spread: 0.0001,
                vbid: 0,
                vask: 0,
                velocity: 0.0,
                dispersion: 0.0,
                drift: 0.0,
            };
        }

        // Recalculate OHLC with valid ticks only (after outlier filtering)
        let timestamp = aggregate_timestamp;
        
        // Recalculate OHLC from valid ticks
        let valid_mids: Vec<f64> = valid_ticks.iter()
            .map(|t| (t.bid + t.ask) / 2.0)
            .collect();
        
        let final_open = round_to_6_sig_digits(*valid_mids.first().unwrap());
        let final_close = round_to_6_sig_digits(*valid_mids.last().unwrap());
        let final_high = round_to_6_sig_digits(
            valid_mids.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
        );
        let final_low = round_to_6_sig_digits(
            valid_mids.iter().cloned().fold(f64::INFINITY, f64::min)
        );
        
        // Using final_close instead of VWAP mid calculation
        
        // Recompute spread using valid ticks only
        let spread = if valid_ticks.len() > 1000 {
            valid_ticks
                .par_iter()
                .map(|tick| {
                    let m = (tick.bid + tick.ask) / 2.0;
                    (tick.ask - tick.bid) / m
                })
                .sum::<f64>() / valid_ticks.len() as f64
        } else {
            valid_ticks
                .iter()
                .map(|tick| {
                    let m = (tick.bid + tick.ask) / 2.0;
                    (tick.ask - tick.bid) / m
                })
                .sum::<f64>() / valid_ticks.len() as f64
        };
        
        // Sum volumes from valid ticks only (both sides accumulate within the bucket)
        let vbid: u32 = valid_ticks.iter().map(|t| t.vbid as u64).sum::<u64>().min(u32::MAX as u64) as u32;
        let vask: u32 = valid_ticks.iter().map(|t| t.vask as u64).sum::<u64>().min(u32::MAX as u64) as u32;
        
        // Calculate velocity: sqrt of number of underlying ticks in bucket
        let velocity = (valid_ticks.len() as f32).sqrt();
        
        // Calculate dispersion and drift using valid ticks
        let (dispersion, drift) = self.calculate_metrics_valid(&valid_ticks);

        // Update state - store the closing price for gap filling
        self.last_aggregate_price = final_close;
        self.current_bucket.clear();
        
        Aggregate {
            timestamp,
            open: final_open,
            high: final_high,
            low: final_low,
            close: final_close,
            mid: final_close,  // alias for close (standard bid|ask|mid format)
            spread: spread as f32,
            vbid,
            vask,
            velocity,
            dispersion,
            drift,
        }
    }


    fn calculate_metrics_valid(&self, ticks: &[&Tick]) -> (f32, f32) {
        if ticks.len() < 2 {
            return (0.0, 0.0);
        }

        // Prepare data
        let n = ticks.len();
        let first_timestamp = ticks[0].timestamp as f64;

        let x_values: Vec<f64> = ticks
            .iter()
            .map(|t| (t.timestamp as f64 - first_timestamp) / 1000.0)
            .collect();
        let y_values: Vec<f64> = ticks
            .iter()
            .map(|t| (t.bid + t.ask) / 2.0)
            .collect();

        let y_mean = y_values.iter().sum::<f64>() / n as f64;

        // Dispersion: standard deviation of mids normalized by mean
        let variance = if n > 1000 {
            y_values
                .par_iter()
                .map(|y| (y - y_mean).powi(2))
                .sum::<f64>()
                / n as f64
        } else {
            y_values
                .iter()
                .map(|y| (y - y_mean).powi(2))
                .sum::<f64>()
                / n as f64
        };
        let dispersion = if y_mean.abs() < 1e-10 { 0.0 } else { ((variance.sqrt() / y_mean) * 100.0) as f32 };

        // Drift: linear regression slope over interval normalized by close price
        let x_mean = x_values.iter().sum::<f64>() / n as f64;
        let mut numerator = 0.0;
        let mut denominator = 0.0;
        for i in 0..n {
            numerator += (x_values[i] - x_mean) * (y_values[i] - y_mean);
            denominator += (x_values[i] - x_mean).powi(2);
        }
        let slope = if denominator != 0.0 { numerator / denominator } else { 0.0 };
        let close_price = *y_values.last().unwrap();
        let time_duration = x_values.last().unwrap() - x_values.first().unwrap();
        let drift = if close_price.abs() < 1e-10 || time_duration <= 0.0 {
            0.0
        } else {
            ((slope * time_duration / close_price) * 100.0) as f32
        };

        (dispersion, drift)
    }
}

#[allow(dead_code)]
pub fn aggregate_multi_source(
    source_ticks: Vec<Vec<Tick>>,
    config: &Config,
) -> Vec<Aggregate> {
    // Merge and sort ticks from multiple sources
    let mut all_ticks: Vec<Tick> = source_ticks
        .into_iter()
        .flatten()
        .collect();
    
    all_ticks.par_sort_unstable_by_key(|t| t.timestamp);
    
    // Create aggregator and process
    let mut aggregator = Aggregator::new(config.clone());
    let mut aggregates = aggregator.process_ticks(&all_ticks);
    
    if let Some(final_agg) = aggregator.finalize() {
        aggregates.push(final_agg);
    }
    
    aggregates
}