mod aggregation;
mod cache;
mod cli;
mod output;
mod sources;
mod types;

use aggregation::Aggregator;
use cache::CacheManager;
use cli::{parse_data_source, Args};
use output::OutputWriter;
use sources::create_source;
use types::{Aggregate, Config, DataSource, Tick};


use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Initialize Rayon thread pool
    let num_threads = std::thread::available_parallelism()
        .map(|x| x.get())
        .unwrap_or(4)
        .max(4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|i| format!("rayon-worker-{}", i))
        .build_global()
        .expect("Failed to initialize Rayon thread pool");
    
    info!("Initialized Rayon thread pool with {} threads", num_threads);

    let args = Args::parse();
    let config = args.into_config()?;

    info!("Starting series factory with config: {:?}", config);
    info!("Output directory: {}", config.output_dir.display());

    // Create cache manager (for raw data caching only)
    let cache_manager = CacheManager::new(config.cache_dir.clone())?;

    // Parse data sources
    let data_sources: Result<Vec<DataSource>> = config
        .sources
        .iter()
        .map(|s| parse_data_source(s))
        .collect();
    let data_sources = data_sources?;

    // Create tick sources
    let mut tick_sources = Vec::new();
    for data_source in data_sources.iter() {
        tick_sources.push(create_source(data_source).await?);
    }

    // Create channels for streaming pipeline
    let (tick_tx, mut tick_rx) = mpsc::channel::<Vec<Tick>>(100);
    let (agg_tx, mut agg_rx) = mpsc::channel::<Vec<Aggregate>>(50);

    // Start fetching ticks from all sources concurrently
    let config_arc = Arc::new(config.clone());
    let fetch_tasks: Vec<_> = tick_sources
        .into_iter()
        .map(|source| {
            let tx = tick_tx.clone();
            let config = config_arc.clone();
            tokio::spawn(async move {
                if let Err(e) = source.fetch_ticks(&config, tx).await {
                    error!("Error fetching ticks: {}", e);
                }
            })
        })
        .collect();

    // Drop the original sender so the receiver can detect when all sources are done
    drop(tick_tx);

    // Create streaming aggregation pipeline
    let config_for_agg = config_arc.clone();
    let aggregation_task = tokio::spawn(async move {
        // Collect all ticks first, then split into thread-sized batches for maximum performance
        let mut all_ticks = Vec::new();

        while let Some(tick_batch) = tick_rx.recv().await {
            all_ticks.extend(tick_batch);
        }
        let total_ticks = all_ticks.len();

        if all_ticks.is_empty() {
            return;
        }

        info!("Collected {} ticks, sorting chronologically", total_ticks);
        
        // Sort all ticks chronologically using parallel sort
        all_ticks.par_sort_unstable_by_key(|t| t.timestamp);

        info!("Processing ticks sequentially to ensure correct bucket aggregation");
        
        // Process sequentially to avoid bucket boundary artifacts
        let aggregates = process_ticks_sequential(&all_ticks, &config_for_agg);
        let aggregate_count = aggregates.len();
        
        if !aggregates.is_empty() {
            let _ = agg_tx.send(aggregates).await;
        }

        info!("Processed {} total ticks into {} aggregates", total_ticks, aggregate_count);
    });

    // Collect aggregates from the streaming pipeline
    let mut all_aggregates = Vec::new();
    while let Some(aggregate_batch) = agg_rx.recv().await {
        all_aggregates.extend(aggregate_batch);
    }

    // Wait for all tasks to complete
    for task in fetch_tasks {
        task.await?;
    }
    aggregation_task.await?;
    
    // Sort by timestamp and deduplicate
    all_aggregates.sort_by_key(|a| a.timestamp);
    all_aggregates.dedup_by_key(|a| a.timestamp);

    if all_aggregates.is_empty() {
        warn!("No aggregates generated. Exiting.");
        return Ok(());
    }

    info!("Generated {} aggregates", all_aggregates.len());

    // Write output
    let output_writer = OutputWriter::new(cache_manager);
    let output_path = output_writer.write_aggregates(&config, &all_aggregates).await?;

    // Display head and tail of the generated data
    display_data_table(&all_aggregates);

    info!("Series factory completed successfully!");
    if let Some(filename) = output_path.file_name() {
        info!("Generated: {}", filename.to_string_lossy());
    }
    info!("Total aggregates: {}", all_aggregates.len());

    Ok(())
}


// Correct tick processing: sequential aggregation to avoid bucket boundary artifacts
fn process_ticks_sequential(all_ticks: &[Tick], config: &Config) -> Vec<Aggregate> {
    if all_ticks.is_empty() {
        return Vec::new();
    }

    let mut aggregator = Aggregator::new(config.clone());
    let mut aggregates = aggregator.process_ticks(all_ticks);
    if let Some(final_agg) = aggregator.finalize() {
        aggregates.push(final_agg);
    }
    
    // Filter out placeholder aggregates with zero price
    aggregates.retain(|agg| agg.close > 0.0);
    
    // Ensure complete time coverage
    aggregates = ensure_complete_time_coverage(aggregates, config);
    
    aggregates
}

// Ensure every time interval has a data point
fn ensure_complete_time_coverage(mut aggregates: Vec<Aggregate>, config: &Config) -> Vec<Aggregate> {
    use crate::types::AggregationMode;
    
    // Only apply to time-based aggregation
    if config.agg_mode != AggregationMode::Time {
        return aggregates;
    }
    
    if aggregates.is_empty() {
        return aggregates;
    }
    
    // Sort by timestamp
    aggregates.sort_by_key(|a| a.timestamp);
    
    let step = config.agg_step as i64;
    let start_time = (config.from.timestamp_millis() / step) * step + step; // First aligned timestamp
    let end_time = (config.to.timestamp_millis() / step) * step + step; // Last aligned timestamp
    
    let mut complete_aggregates = Vec::new();
    let mut last_price = 0.0;
    let mut agg_iter = aggregates.into_iter();
    let mut next_agg = agg_iter.next();
    
    let mut current_time = start_time;
    
    while current_time <= end_time {
        if let Some(ref agg) = next_agg {
            if agg.timestamp == current_time {
                // We have data for this timestamp
                last_price = agg.close;
                complete_aggregates.push(agg.clone());
                next_agg = agg_iter.next();
            } else if agg.timestamp > current_time {
                // Gap - create filler aggregate if we have a price
                if last_price > 0.0 {
                    complete_aggregates.push(Aggregate {
                        timestamp: current_time,
                        open: last_price,
                        high: last_price,
                        low: last_price,
                        close: last_price,
                        mid: last_price,
                        spread: 0.0001,
                        vbid: 0,
                        vask: 0,
                        velocity: 0.0,
                        dispersion: 0.0,
                        drift: 0.0,
                    });
                }
            } else {
                // agg.timestamp < current_time - shouldn't happen if sorted properly
                next_agg = agg_iter.next();
                continue;
            }
        } else if last_price > 0.0 {
            // No more aggregates, fill to the end
            complete_aggregates.push(Aggregate {
                timestamp: current_time,
                open: last_price,
                high: last_price,
                low: last_price,
                close: last_price,
                mid: last_price,
                spread: 0.0001,
                vbid: 0,
                vask: 0,
                velocity: 0.0,
                dispersion: 0.0,
                drift: 0.0,
            });
        }
        
        current_time += step;
    }
    
    complete_aggregates
}


fn display_data_table(aggregates: &[Aggregate]) {
    use chrono::DateTime;
    
    if aggregates.is_empty() {
        println!("No data to display");
        return;
    }
    
    println!("\n{}", "=".repeat(120));
    println!("                                    GENERATED DATA PREVIEW");
    println!("{}", "=".repeat(120));
    
    // Header
    println!("{:<20} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}", 
        "Timestamp", "Mid", "Spread", "VBid", "VAsk", "Velocity", "Dispersion", "Drift");
    println!("{}", "-".repeat(120));
    
    // Display first 10 rows
    println!("=== FIRST 10 ROWS ===");
    for (i, agg) in aggregates.iter().take(10).enumerate() {
        // Debug: print raw timestamp for first aggregate
        if i == 0 {
            println!("DEBUG: First aggregate timestamp = {}", agg.timestamp);
        }
        let dt = DateTime::from_timestamp_millis(agg.timestamp).unwrap_or_default();
        let formatted_time = dt.format("%Y%m%d %H:%M:%S%.3f").to_string();
        
        // Use adaptive precision for close price display
        let price_str = if agg.close < 1.0 {
            format!("{:.8}", agg.close)  // 8 decimal places for small numbers
        } else if agg.close < 100.0 {
            format!("{:.6}", agg.close)  // 6 decimal places for medium numbers  
        } else {
            format!("{:.4}", agg.close)  // 4 decimal places for large numbers
        };
        
        println!("{:<20} {:>12} {:>12.6} {:>12} {:>12} {:>12.6} {:>12.6} {:>12.6}",
            formatted_time,
            price_str,
            agg.spread,
            agg.vbid,
            agg.vask,
            agg.velocity,
            agg.dispersion,
            agg.drift
        );
    }
    
    // Display last 10 rows if we have more than 10 rows
    if aggregates.len() > 10 {
        println!("\n=== LAST 10 ROWS ===");
        for agg in aggregates.iter().rev().take(10).rev() {
            let dt = DateTime::from_timestamp_millis(agg.timestamp).unwrap_or_default();
            let formatted_time = dt.format("%Y%m%d %H:%M:%S%.3f").to_string();
            
            // Use adaptive precision for close price display
            let price_str = if agg.close < 1.0 {
                format!("{:.8}", agg.close)  // 8 decimal places for small numbers
            } else if agg.close < 100.0 {
                format!("{:.6}", agg.close)  // 6 decimal places for medium numbers  
            } else {
                format!("{:.4}", agg.close)  // 4 decimal places for large numbers
            };
            
            println!("{:<20} {:>12} {:>12.6} {:>12} {:>12} {:>12.6} {:>12.6} {:>12.6}",
                formatted_time,
                price_str,
                agg.spread,
                agg.vbid,
                agg.vask,
                agg.velocity,
                agg.dispersion,
                agg.drift
            );
        }
    }
    
    println!("{}", "=".repeat(120));
    println!("Total rows: {}", aggregates.len());
    
    // Summary statistics
    if !aggregates.is_empty() {
        let avg_price: f64 = aggregates.iter().map(|a| a.close).sum::<f64>() / aggregates.len() as f64;
        let avg_spread: f32 = aggregates.iter().map(|a| a.spread).sum::<f32>() / aggregates.len() as f32;
        let total_volume: u64 = aggregates.iter().map(|a| (a.vbid + a.vask) as u64).sum();
        
        println!("Average Price: {:.4}", avg_price);
        println!("Average Spread: {:.6}", avg_spread);
        println!("Total Volume: {}", total_volume);
    }
    println!("{}", "=".repeat(120));
}
