use series_factory::types::{AggregationMode, Config, DataSource, GenerativeModel};
use series_factory::aggregation::Aggregator;
use series_factory::sources::create_source;
use chrono::{Duration, Utc};
use tokio::sync::mpsc;

const TEST_STEP_MS: i64 = 10_000;

/// Base test config builder
fn test_config() -> Config {
    Config {
        base: "BTC".to_string(),
        quote: "USDT".to_string(),
        sources: vec![],
        from: Utc::now() - Duration::days(30),
        to: Utc::now() - Duration::days(29),
        agg_mode: AggregationMode::Time,
        agg_step: TEST_STEP_MS as f64,
        agg_fields: vec!["all".to_string()],
        weight_mode: series_factory::types::WeightMode::Static,
        weights: vec![1.0],
        source_weights: vec![],
        tick_ttl: 5000,
        tick_max_deviation: 0.05,
        out_format: "parquet".to_string(),
        cache_dir: "/tmp/test_cache".into(),
        output_dir: "/tmp/test_output".into(),
    }
}

/// Generic time aggregation test for any exchange
async fn test_exchange_time_aggregation(exchange: &str, days_back: i64) {
    let mut config = test_config();
    config.sources = vec![exchange.to_string()];
    config.from = Utc::now() - Duration::days(days_back);
    config.to = Utc::now() - Duration::days(days_back - 1);

    let source = create_source(&DataSource::Exchange(exchange.to_string()))
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let config_for_spawn = config.clone();
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config_for_spawn, tx).await;
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
        assert_eq!(diff, TEST_STEP_MS, "{} time aggregates should have consistent steps", exchange);
    }
}

/// Fetch ticks from a source
async fn fetch_ticks(source: DataSource, from: chrono::DateTime<Utc>, to: chrono::DateTime<Utc>) -> Vec<series_factory::types::Tick> {
    let src = create_source(&source).await.unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let mut config = test_config();
    config.from = from;
    config.to = to;

    tokio::spawn(async move {
        let _ = src.fetch_ticks(&config, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 10_000 { break; }
    }
    ticks
}

#[tokio::test]
async fn test_binance_time_aggregation() {
    test_exchange_time_aggregation("binance", 30).await;
}

#[tokio::test]
async fn test_bybit_time_aggregation() {
    test_exchange_time_aggregation("bybit", 30).await;
}

#[tokio::test]
async fn test_okx_time_aggregation() {
    test_exchange_time_aggregation("okx", 30).await;
}

#[tokio::test]
async fn test_kucoin_time_aggregation() {
    test_exchange_time_aggregation("kucoin", 30).await;
}

#[tokio::test]
async fn test_bitget_time_aggregation() {
    test_exchange_time_aggregation("bitget", 30).await;
}

#[tokio::test]
async fn test_gbm_price_aggregation() {
    let mut config = test_config();
    config.sources = vec!["gbm".to_string()];
    config.from = Utc::now() - Duration::hours(1);
    config.to = Utc::now();
    config.agg_mode = AggregationMode::Tick;
    config.agg_step = 0.001;

    let model = GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 };
    let source = create_source(&DataSource::Synthetic(model)).await.unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let config_for_spawn = config.clone();
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config_for_spawn, tx).await;
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
        let expected_upper = prev_mid * 1.001;
        let expected_lower = prev_mid / 1.001;
        assert!(curr_mid >= expected_upper || curr_mid <= expected_lower);
    }
}

#[tokio::test]
async fn test_all_synthetic_models() {
    let models = vec![
        GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 },
        GenerativeModel::FBM { mu: 0.0001, sigma: 0.001, hurst: 0.7, base: 100.0 },
        GenerativeModel::Heston {
            mu: 0.0001, sigma: 0.001, kappa: 1000.0, theta: 0.001, xi: 0.001, rho: -0.75, base: 100.0
        },
        GenerativeModel::NormalJumpDiffusion {
            mu: 0.0001, sigma: 0.001, lambda: 10.0, mu_jump: 0.0, sigma_jump: 0.1, base: 100.0
        },
        GenerativeModel::DoubleExpJumpDiffusion {
            mu: 0.0001, sigma: 0.001, lambda: 10.0, mu_pos_jump: 0.01, mu_neg_jump: -0.02, p_neg_jump: 0.6, base: 100.0
        },
    ];

    let from = Utc::now() - Duration::days(1);
    let to = Utc::now();

    let tasks: Vec<_> = models.into_iter().map(|model| {
        tokio::spawn(async move {
            let mut config = test_config();
            config.from = from;
            config.to = to;

            let source = create_source(&DataSource::Synthetic(model)).await.unwrap();
            let (tx, mut rx) = mpsc::channel(100);

            let config_for_spawn = config.clone();
            tokio::spawn(async move {
                let _ = source.fetch_ticks(&config_for_spawn, tx).await;
            });

            let mut ticks = Vec::new();
            while let Some(batch) = rx.recv().await {
                ticks.extend(batch);
                if ticks.len() > 1000 { break; }
            }

            assert!(!ticks.is_empty());

            let mut aggregator = Aggregator::new(config);
            let results = aggregator.process_ticks(&ticks);
            assert!(!results.is_empty());

            for agg in &results {
                assert!(agg.timestamp > 0);
                assert!(agg.close > 0.0);
                assert!(agg.velocity >= 0.0);
            }
        })
    }).collect();

    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn test_aggregate_fields() {
    let mut config = test_config();
    config.from = Utc::now() - Duration::hours(1);
    config.to = Utc::now();

    let model = GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 };
    let source = create_source(&DataSource::Synthetic(model)).await.unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let config_for_spawn = config.clone();
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config_for_spawn, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 1000 { break; }
    }

    let mut aggregator = Aggregator::new(config);
    let results = aggregator.process_ticks(&ticks);

    for agg in &results {
        assert!(agg.timestamp > 0);
        assert!(agg.open > 0.0);
        assert!(agg.high >= agg.low);
        assert!(agg.close > 0.0);
        assert!(agg.mid > 0.0);
        assert!(agg.spread >= 0.0);
        assert!(agg.velocity >= 0.0);
        assert!(agg.high >= agg.open && agg.high >= agg.close);
        assert!(agg.low <= agg.open && agg.low <= agg.close);
    }
}
