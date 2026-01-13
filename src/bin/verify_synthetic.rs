use series_factory::types::{AggregationMode, Config, DataSource, GenerativeModel};
use series_factory::aggregation::aggregate_multi_source;
use series_factory::sources::create_source;
use chrono::{Duration, Utc};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let models = vec![
        ("GBM", GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 }),
        ("FBM", GenerativeModel::FBM { mu: 0.0001, sigma: 0.001, hurst: 0.7, base: 100.0 }),
        ("Heston", GenerativeModel::Heston {
            mu: 0.0001, sigma: 0.001, kappa: 1000.0, theta: 0.001, xi: 0.001, rho: -0.75, base: 100.0
        }),
        ("NJDM", GenerativeModel::NormalJumpDiffusion {
            mu: 0.0001, sigma: 0.001, lambda: 10.0, mu_jump: 0.0, sigma_jump: 0.1, base: 100.0
        }),
        ("DEJDM", GenerativeModel::DoubleExpJumpDiffusion {
            mu: 0.0001, sigma: 0.001, lambda: 10.0, mu_pos_jump: 0.01, mu_neg_jump: -0.02, p_neg_jump: 0.6, base: 100.0
        }),
    ];

    for (name, model) in models {
        println!("\n=== Testing {} ===", name);

        let source = create_source(&DataSource::Synthetic(model.clone())).await?;
        let config = Config {
            base: "TEST".to_string(),
            quote: "USD".to_string(),
            sources: vec![name.to_string()],
            from: Utc::now() - Duration::hours(1),
            to: Utc::now(),
            agg_mode: AggregationMode::Time,
            agg_step: 1000.0, // 1 second
            agg_fields: vec!["all".to_string()],
            weight_mode: series_factory::types::WeightMode::Static,
            weights: vec![1.0],
            source_weights: vec![1.0],
            tick_max_deviation: 0.05,
            cache_dir: "/tmp/verify_cache".into(),
            output_dir: "/tmp/verify_output".into(),
        };

        let (tx, mut rx) = mpsc::channel(100);
        let config_clone = config.clone();
        tokio::spawn(async move {
            let _ = source.fetch_ticks(&config_clone, tx).await;
        });

        let mut ticks = Vec::new();
        while let Some(batch) = rx.recv().await {
            ticks.extend(batch);
            if ticks.len() >= 5000 { break; }
        }

        println!("  Generated {} ticks", ticks.len());

        if ticks.is_empty() {
            println!("  ERROR: No ticks generated!");
            continue;
        }

        // Show sample ticks
        println!("  Sample ticks (first 3):");
        for (i, tick) in ticks.iter().take(3).enumerate() {
            let mid = (tick.bid + tick.ask) / 2.0;
            println!("    [{}]: ts={}, bid={:.6}, ask={:.6}, mid={:.6}",
                i, tick.timestamp, tick.bid, tick.ask, mid);
        }

        // Aggregate and verify
        let all_results = aggregate_multi_source(vec![ticks], &config);

        println!("  Generated {} aggregates", all_results.len());

        if all_results.is_empty() {
            println!("  ERROR: No aggregates generated!");
            continue;
        }

        // Show sample aggregates with all fields
        println!("  Sample aggregates (first 3):");
        for (i, agg) in all_results.iter().take(3).enumerate() {
            println!("    [{}]: ts={}, open={:.6}, high={:.6}, low={:.6}, close={:.6}, spread={:.6}, velocity={:.2}, dispersion={:.4}, drift={:.4}",
                i, agg.timestamp, agg.open, agg.high, agg.low, agg.close, agg.spread, agg.velocity, agg.dispersion, agg.drift);
        }

        // Verify field constraints
        let mut valid = true;
        for agg in &all_results {
            if agg.high < agg.low || agg.close <= 0.0 || agg.velocity < 0.0 {
                valid = false;
                break;
            }
        }
        println!("  Validation: {}", if valid { "PASS ✓" } else { "FAIL ✗" });
    }

    println!("\n=== All synthetic models verified ===");
    Ok(())
}
