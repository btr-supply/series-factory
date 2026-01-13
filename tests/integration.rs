use series_factory::types::{Aggregate, AggregationMode, Config, DataSource, GenerativeModel};
use series_factory::aggregation::Aggregator;
use series_factory::sources::create_source;
use chrono::{Duration, Utc};
use futures::future::try_join_all;
use tokio::sync::mpsc;

const TEST_SYMBOL: &str = "BTCUSDT";
const TEST_STEP_MS: i64 = 10_000; // 10 seconds

/// Time aggregation test: consistent timestamp steps
async fn test_time_aggregation(config: Config) {
    let source = create_source(&DataSource::Exchange("binance".to_string()))
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 10_000 { break; }
    }

    if ticks.is_empty() { return; }

    let mut aggregator = Aggregator::new(config);
    let aggregates = aggregator.process_ticks(&ticks);
    let final_agg = aggregator.finalize();
    let mut results = aggregates;
    if let Some(agg) = final_agg {
        if agg.close > 0.0 { results.push(agg); }
    }

    // Verify consistent timestamp steps
    for window in results.windows(2) {
        let diff = window[1].timestamp - window[0].timestamp;
        assert_eq!(diff, TEST_STEP_MS, "Time aggregates should have consistent steps");
    }
}

/// Price aggregation test: consistent mid steps (Renko-style)
async fn test_price_aggregation(config: Config, step_ratio: f64) {
    let (tx, mut rx) = mpsc::channel(100);

    // Use GBM for fast synthetic data
    let model = GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 };
    let source = create_source(&DataSource::Synthetic(model)).await.unwrap();

    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 10_000 { break; }
    }

    let mut aggregator = Aggregator::new(config);
    let aggregates = aggregator.process_ticks(&ticks);
    let final_agg = aggregator.finalize();
    let mut results = aggregates;
    if let Some(agg) = final_agg {
        if agg.close > 0.0 { results.push(agg); }
    }

    // Verify Renko-style price steps
    for window in results.windows(2) {
        let prev_mid = window[0].mid;
        let curr_mid = window[1].mid;
        let expected_upper = prev_mid * (1.0 + step_ratio);
        let expected_lower = prev_mid / (1.0 + step_ratio);

        // Current mid should be outside previous brick range
        let outside_range = curr_mid >= expected_upper || curr_mid <= expected_lower;
        assert!(outside_range, "Price aggregates should form Renko bricks");
    }
}

/// Test all synthetic generative models
async fn test_synthetic_models() {
    let from = Utc::now() - Duration::days(1);
    let to = Utc::now();

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

    let tasks: Vec<_> = models.into_iter().map(|(name, model)| {
        tokio::spawn(async move {
            let config = Config {
                base: "BTC".to_string(),
                quote: "USDT".to_string(),
                sources: vec![name.to_string()],
                from,
                to,
                agg_mode: AggregationMode::Time,
                agg_step: TEST_STEP_MS as f64,
                agg_fields: vec!["all".to_string()],
                weight_mode: series_factory::types::WeightMode::Static,
                weights: vec![1.0],
                tick_ttl: 5000,
                tick_max_deviation: 0.05,
                out_format: "parquet".to_string(),
                cache_dir: "/tmp/test_cache".into(),
                output_dir: "/tmp/test_output".into(),
            };

            let source = create_source(&DataSource::Synthetic(model)).await.unwrap();
            let (tx, mut rx) = mpsc::channel(100);

            tokio::spawn(async move {
                let _ = source.fetch_ticks(&config, tx).await;
            });

            let mut ticks = Vec::new();
            while let Some(batch) = rx.recv().await {
                ticks.extend(batch);
                if ticks.len() > 1000 { break; }
            }

            assert!(!ticks.is_empty(), "{} should generate ticks", name);

            let mut aggregator = Aggregator::new(config);
            let results = aggregator.process_ticks(&ticks);
            assert!(!results.is_empty(), "{} should produce aggregates", name);

            // Verify required fields
            for agg in &results {
                assert!(agg.timestamp > 0);
                assert!(agg.close > 0.0);
                assert!(agg.velocity >= 0.0);
            }
        })
    }).collect();

    try_join_all(tasks).await.unwrap();
}

#[tokio::test]
async fn test_binance_time_aggregation() {
    let config = Config {
        base: "BTC".to_string(),
        quote: "USDT".to_string(),
        sources: vec!["binance".to_string()],
        from: Utc::now() - Duration::days(30),
        to: Utc::now() - Duration::days(29),
        agg_mode: AggregationMode::Time,
        agg_step: TEST_STEP_MS as f64,
        agg_fields: vec!["all".to_string()],
        weight_mode: series_factory::types::WeightMode::Static,
        weights: vec![1.0],
        tick_ttl: 5000,
        tick_max_deviation: 0.05,
        out_format: "parquet".to_string(),
        cache_dir: "/tmp/test_cache".into(),
        output_dir: "/tmp/test_output".into(),
    };

    test_time_aggregation(config).await;
}

#[tokio::test]
async fn test_gbm_price_aggregation() {
    let config = Config {
        base: "BTC".to_string(),
        quote: "USDT".to_string(),
        sources: vec!["gbm".to_string()],
        from: Utc::now() - Duration::hours(1),
        to: Utc::now(),
        agg_mode: AggregationMode::Tick,
        agg_step: 0.001, // 0.1% price step
        agg_fields: vec!["all".to_string()],
        weight_mode: series_factory::types::WeightMode::Static,
        weights: vec![1.0],
        tick_ttl: 5000,
        tick_max_deviation: 0.05,
        out_format: "parquet".to_string(),
        cache_dir: "/tmp/test_cache".into(),
        output_dir: "/tmp/test_output".into(),
    };

    test_price_aggregation(config, 0.001).await;
}

#[tokio::test]
async fn test_all_synthetic_models() {
    test_synthetic_models().await;
}

#[tokio::test]
async fn test_aggregate_fields() {
    let model = GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 };
    let source = create_source(&DataSource::Synthetic(model)).await.unwrap();

    let config = Config {
        base: "BTC".to_string(),
        quote: "USDT".to_string(),
        sources: vec!["test".to_string()],
        from: Utc::now() - Duration::hours(1),
        to: Utc::now(),
        agg_mode: AggregationMode::Time,
        agg_step: TEST_STEP_MS as f64,
        agg_fields: vec!["all".to_string()],
        weight_mode: series_factory::types::WeightMode::Static,
        weights: vec![1.0],
        tick_ttl: 5000,
        tick_max_deviation: 0.05,
        out_format: "parquet".to_string(),
        cache_dir: "/tmp/test_cache".into(),
        output_dir: "/tmp/test_output".into(),
    };

    let (tx, mut rx) = mpsc::channel(100);
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 1000 { break; }
    }

    let mut aggregator = Aggregator::new(config);
    let results = aggregator.process_ticks(&ticks);

    // Verify all aggregate fields are present and valid
    for agg in &results {
        assert!(agg.timestamp > 0, "timestamp must be positive");
        assert!(agg.open > 0.0, "open must be positive");
        assert!(agg.high >= agg.low, "high >= low");
        assert!(agg.close > 0.0, "close must be positive");
        assert!(agg.mid > 0.0, "mid must be positive");
        assert!(agg.spread >= 0.0, "spread must be non-negative");
        assert!(agg.velocity >= 0.0, "velocity must be non-negative");
        assert!(agg.high >= agg.open && agg.high >= agg.close, "high must be >= open,close");
        assert!(agg.low <= agg.open && agg.low <= agg.close, "low must be <= open,close");
    }
}
