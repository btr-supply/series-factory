mod aggregation;
mod cli;
mod display;
mod output;
mod sources;
mod types;

use aggregation::Aggregator;
use cli::{parse_data_source, Args};
use display::display_data_table;
use output::OutputWriter;
use sources::create_source;
use types::{Aggregate, Config, DataSource, Tick, DEFAULT_SPREAD, STREAMING_BUFFER_SIZE};


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

    // Create streaming aggregation pipeline with windowed processing
    let config_for_agg = config_arc.clone();
    let aggregation_task = tokio::spawn(async move {
        let mut aggregator = Aggregator::new((*config_for_agg).clone());
        let mut total_ticks = 0;
        let mut tick_buffer = Vec::new();

        while let Some(tick_batch) = tick_rx.recv().await {
            let batch_len = tick_batch.len();
            tick_buffer.extend(tick_batch);
            total_ticks += batch_len;

            // Process buffer when it reaches threshold
            if tick_buffer.len() >= STREAMING_BUFFER_SIZE {
                // Sort current buffer
                tick_buffer.par_sort_unstable_by_key(|t| t.timestamp);

                // Stream through aggregator
                let batch_aggregates = aggregator.process_ticks(&tick_buffer);
                if !batch_aggregates.is_empty() {
                    let _ = agg_tx.send(batch_aggregates).await;
                }

                tick_buffer.clear();
            }
        }

        // Process remaining ticks
        if !tick_buffer.is_empty() {
            tick_buffer.par_sort_unstable_by_key(|t| t.timestamp);
            let batch_aggregates = aggregator.process_ticks(&tick_buffer);
            if !batch_aggregates.is_empty() {
                let _ = agg_tx.send(batch_aggregates).await;
            }
        }

        // Finalize aggregator
        if let Some(final_agg) = aggregator.finalize() {
            let _ = agg_tx.send(vec![final_agg]).await;
        }

        info!("Processed {} total ticks via streaming aggregation", total_ticks);
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

    // Filter out placeholder aggregates with zero price
    all_aggregates.retain(|agg| agg.close > 0.0);

    // Ensure complete time coverage with synthetic marking
    all_aggregates = ensure_complete_time_coverage(all_aggregates, &config);

    if all_aggregates.is_empty() {
        warn!("No aggregates generated. Exiting.");
        return Ok(());
    }

    info!("Generated {} aggregates", all_aggregates.len());

    // Write output
    let output_writer = OutputWriter::new();
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
                        spread: DEFAULT_SPREAD,
                        vbid: 0,
                        vask: 0,
                        velocity: 0.0,
                        dispersion: 0.0,
                        drift: 0.0,
                        is_synthetic: true,
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
                is_synthetic: true,
            });
        }
        
        current_time += step;
    }
    
    complete_aggregates
}
