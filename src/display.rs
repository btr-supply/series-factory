use crate::types::Aggregate;
use chrono::DateTime;

/// Display a preview of the generated aggregates
pub fn display_data_table(aggregates: &[Aggregate]) {
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
    for agg in aggregates.iter().take(10) {
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
        let synthetic_count = aggregates.iter().filter(|a| a.is_synthetic).count();

        println!("Average Price: {:.4}", avg_price);
        println!("Average Spread: {:.6}", avg_spread);
        println!("Total Volume: {}", total_volume);
        println!("Synthetic Candles: {} ({:.2}%)", synthetic_count,
                 100.0 * synthetic_count as f64 / aggregates.len() as f64);
    }
    println!("{}", "=".repeat(120));
}
