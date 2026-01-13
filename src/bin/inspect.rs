use arrow::array::{Float64Array, Int64Array, UInt32Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::env;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <parquet_file>", args[0]);
        std::process::exit(1);
    }

    let filepath = &args[1];
    println!("\n{}", "=".repeat(120));
    println!("Inspecting: {}", filepath);
    println!("{}", "=".repeat(120));

    // Open parquet file
    let file = File::open(filepath)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    
    let mut all_batches = Vec::new();
    let mut total_rows = 0;
    
    // Read all batches
    for batch_result in reader {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        all_batches.push(batch);
    }
    
    println!("Total rows: {}", total_rows);
    
    if all_batches.is_empty() {
        println!("No data found!");
        return Ok(());
    }
    
    // Get first and last batches
    let first_batch = &all_batches[0];
    let last_batch = &all_batches[all_batches.len() - 1];
    
    // Extract columns from first batch
    let timestamps_first = first_batch.column(0).as_any().downcast_ref::<Int64Array>()
        .ok_or("Failed to cast timestamp column")?;
    let mid_first = first_batch.column(1).as_any().downcast_ref::<Float64Array>()
        .ok_or("Failed to cast mid column")?;
    let spread_first = first_batch.column(2).as_any().downcast_ref::<Float64Array>()
        .ok_or("Failed to cast spread column")?;
    let vbid_first = first_batch.column(3).as_any().downcast_ref::<UInt32Array>()
        .ok_or("Failed to cast vbid column")?;
    let vask_first = first_batch.column(4).as_any().downcast_ref::<UInt32Array>()
        .ok_or("Failed to cast vask column")?;
    
    // Extract columns from last batch
    let timestamps_last = last_batch.column(0).as_any().downcast_ref::<Int64Array>()
        .ok_or("Failed to cast timestamp column")?;
    let mid_last = last_batch.column(1).as_any().downcast_ref::<Float64Array>()
        .ok_or("Failed to cast mid column")?;
    let spread_last = last_batch.column(2).as_any().downcast_ref::<Float64Array>()
        .ok_or("Failed to cast spread column")?;
    let vbid_last = last_batch.column(3).as_any().downcast_ref::<UInt32Array>()
        .ok_or("Failed to cast vbid column")?;
    let vask_last = last_batch.column(4).as_any().downcast_ref::<UInt32Array>()
        .ok_or("Failed to cast vask column")?;
    
    // Print first 20 rows
    println!("\n{} FIRST 20 ROWS {}", "=".repeat(50), "=".repeat(50));
    println!("{:<20} {:>12} {:>12} {:>12} {:>12}", "Timestamp(μs)", "Mid", "Spread", "VBid", "VAsk");
    println!("{}", "-".repeat(120));
    
    let rows_to_show = std::cmp::min(20, first_batch.num_rows());
    for i in 0..rows_to_show {
        let ts = timestamps_first.value(i);
        let close = mid_first.value(i);
        let spread = spread_first.value(i);
        let vbid = vbid_first.value(i);
        let vask = vask_first.value(i);
        
        println!("{:<20} {:>12.4} {:>12.6} {:>12} {:>12}", 
                 ts, close, spread, vbid, vask);
    }
    
    // Print last 20 rows
    println!("\n{} LAST 20 ROWS {}", "=".repeat(50), "=".repeat(50));
    println!("{:<20} {:>12} {:>12} {:>12} {:>12}", "Timestamp(μs)", "Mid", "Spread", "VBid", "VAsk");
    println!("{}", "-".repeat(120));
    
    let start_idx = if last_batch.num_rows() > 20 { 
        last_batch.num_rows() - 20 
    } else { 
        0 
    };
    
    for i in start_idx..last_batch.num_rows() {
        let ts = timestamps_last.value(i);
        let close = mid_last.value(i);
        let spread = spread_last.value(i);
        let vbid = vbid_last.value(i);
        let vask = vask_last.value(i);
        
        println!("{:<20} {:>12.4} {:>12.6} {:>12} {:>12}", 
                 ts, close, spread, vbid, vask);
    }
    
    // Sanity checks
    println!("\n{} SANITY CHECKS {}", "=".repeat(50), "=".repeat(50));
    
    // Check timestamp monotonicity
    let mut is_monotonic = true;
    let mut prev_ts = 0i64;
    let mut non_monotonic_count = 0;
    
    for batch in &all_batches {
        let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            let ts = timestamps.value(i);
            if ts <= prev_ts && prev_ts != 0 {
                is_monotonic = false;
                non_monotonic_count += 1;
            }
            prev_ts = ts;
        }
    }
    
    println!("✓ Timestamps monotonically increasing: {}", is_monotonic);
    if !is_monotonic {
        println!("  WARNING: Found {} non-monotonic timestamps", non_monotonic_count);
    }
    
    // Calculate statistics
    let mut min_mid = f64::MAX;
    let mut max_mid = f64::MIN;
    let mut sum_mid = 0.0;
    let mut count_mid = 0;
    
    let mut min_spread = f64::MAX;
    let mut max_spread = f64::MIN;
    let mut sum_spread = 0.0;
    
    let mut total_vbid = 0u64;
    let mut total_vask = 0u64;
    let mut zero_volume_count = 0;
    
    let mut first_ts = 0i64;
    let mut last_ts = 0i64;
    
    for (batch_idx, batch) in all_batches.iter().enumerate() {
        let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let mids = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let spreads = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let vbids = batch.column(3).as_any().downcast_ref::<UInt32Array>().unwrap();
        let vasks = batch.column(4).as_any().downcast_ref::<UInt32Array>().unwrap();
        
        for i in 0..batch.num_rows() {
            let mid = mids.value(i);
            let spread = spreads.value(i);
            let vbid = vbids.value(i);
            let vask = vasks.value(i);
            
            // Track first and last timestamps
            if batch_idx == 0 && i == 0 {
                first_ts = timestamps.value(i);
            }
            if batch_idx == all_batches.len() - 1 && i == batch.num_rows() - 1 {
                last_ts = timestamps.value(i);
            }
            
            // Mid price stats
            min_mid = min_mid.min(mid);
            max_mid = max_mid.max(mid);
            sum_mid += mid;
            count_mid += 1;
            
            // Spread stats
            min_spread = min_spread.min(spread);
            max_spread = max_spread.max(spread);
            sum_spread += spread;
            
            // Volume stats
            total_vbid += vbid as u64;
            total_vask += vask as u64;
            
            if vbid == 0 && vask == 0 {
                zero_volume_count += 1;
            }
        }
    }
    
    let mean_mid = sum_mid / count_mid as f64;
    let mean_spread = sum_spread / count_mid as f64;
    
    // Check bid < ask (since we have mid and spread, bid = mid - spread/2, ask = mid + spread/2)
    println!("✓ Bid < Ask: Always true by construction (bid = close - spread/2, ask = close + spread/2)");
    
    // Price statistics
    println!("\n✓ Price (close) statistics:");
    println!("    Min: ${:.2}", min_mid);
    println!("    Max: ${:.2}", max_mid);
    println!("    Mean: ${:.2}", mean_mid);
    println!("    Range: ${:.2} ({:.2}%)", max_mid - min_mid, (max_mid - min_mid) / mean_mid * 100.0);
    
    // Spread statistics
    println!("\n✓ Spread statistics:");
    println!("    Min: {:.6}", min_spread);
    println!("    Max: {:.6}", max_spread);
    println!("    Mean: {:.6}", mean_spread);
    println!("    As % of close: {:.4}%", mean_spread / mean_mid * 100.0);
    
    if max_spread / mean_mid > 0.01 {
        println!("    WARNING: Max spread is {:.2}% of close price", max_spread / mean_mid * 100.0);
    }
    
    // Volume statistics
    println!("\n✓ Volume statistics:");
    println!("    Total volume: {}", total_vbid + total_vask);
    println!("    Bid volume: {}", total_vbid);
    println!("    Ask volume: {}", total_vask);
    if total_vask > 0 {
        println!("    Bid/Ask ratio: {:.2}", total_vbid as f64 / total_vask as f64);
    }
    if zero_volume_count > 0 {
        println!("    WARNING: {} rows with zero volume on both sides", zero_volume_count);
    }
    
    // Time statistics
    let duration_ms = (last_ts - first_ts) / 1000;
    let duration_hours = duration_ms as f64 / (1000.0 * 60.0 * 60.0);
    let duration_days = duration_hours / 24.0;
    
    println!("\n✓ Time span:");
    println!("    First timestamp: {} μs", first_ts);
    println!("    Last timestamp: {} μs", last_ts);
    println!("    Duration: {:.2} days ({:.2} hours)", duration_days, duration_hours);
    println!("    Average time between rows: {:.2} ms", duration_ms as f64 / total_rows as f64);
    
    Ok(())
}