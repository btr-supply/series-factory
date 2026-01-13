use crate::types::{Config, Tick, round_to_6_sig_digits};
use crate::sources::TickSource;
use anyhow::Result;
use chrono::NaiveDateTime;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// MEXC aggregated trade response format (same as Binance)
#[derive(Debug, Deserialize)]
struct MexcAggTrade {
    #[serde(rename = "a")]
    agg_trade_id: i64,
    #[serde(rename = "f")]
    first_trade_id: i64,
    #[serde(rename = "l")]
    last_trade_id: i64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    transact_time: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

pub struct MexcSource {
    client: reqwest::Client,
}

impl MexcSource {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .unwrap(),
        }
    }

    fn get_symbol(&self, config: &Config) -> String {
        format!("{}{}", config.base, config.quote)
    }

    /// Fetch aggTrades from MEXC API with time range pagination
    async fn fetch_agg_trades(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<MexcAggTrade>> {
        let url = "https://api.mexc.com/api/v3/aggTrades";
        let mut all_trades = Vec::new();
        let mut current_start = start_time;

        while current_start < end_time {
            let response = self
                .client
                .get(url)
                .query(&[
                    ("symbol", symbol),
                    ("startTime", &current_start.to_string()),
                    ("endTime", &end_time.to_string()),
                    ("limit", &"1000".to_string()),
                ])
                .send()
                .await?
                .json::<Vec<MexcAggTrade>>()
                .await?;

            if response.is_empty() {
                break;
            }

            // Update current_start to the timestamp of the last trade + 1ms
            if let Some(last_trade) = response.last() {
                current_start = last_trade.transact_time + 1;
            } else {
                break;
            }

            all_trades.extend(response);

            // Rate limiting: MEXC allows 120 requests per minute
            // Sleep for 500ms between requests to stay well within limits
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        Ok(all_trades)
    }

    /// Convert MEXC aggTrades to internal Tick format
    fn agg_trades_to_ticks(trades: Vec<MexcAggTrade>) -> Vec<Tick> {
        let mut ticks = Vec::with_capacity(trades.len());
        let mut last_bid = 0.0f64;
        let mut last_ask = 0.0f64;
        let mut is_first_tick = true;

        for agg_trade in trades {
            let price: f64 = agg_trade
                .price
                .parse()
                .expect("Failed to parse price as f64");
            let quantity: f64 = agg_trade
                .quantity
                .parse()
                .expect("Failed to parse quantity as f64");

            // For the first tick, initialize both bid and ask to the same price
            if is_first_tick {
                last_bid = round_to_6_sig_digits(price);
                last_ask = round_to_6_sig_digits(price);
                is_first_tick = false;
            }

            // Convert aggTrade to tick
            let tick = if agg_trade.is_buyer_maker {
                // Market sell - price is bid
                last_bid = round_to_6_sig_digits(price);
                let ask_price = round_to_6_sig_digits(last_ask.max(price * 1.0001));
                Tick {
                    timestamp: agg_trade.transact_time,
                    bid: last_bid,
                    ask: ask_price,
                    vbid: (quantity * price) as u32,
                    vask: 0,
                }
            } else {
                // Market buy - price is ask
                last_ask = round_to_6_sig_digits(price);
                let bid_price = round_to_6_sig_digits(last_bid.min(price * 0.9999));
                Tick {
                    timestamp: agg_trade.transact_time,
                    bid: bid_price,
                    ask: last_ask,
                    vbid: 0,
                    vask: (quantity * price) as u32,
                }
            };

            ticks.push(tick);
        }

        ticks
    }
}

#[async_trait::async_trait]
impl TickSource for MexcSource {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<Tick>>,
    ) -> Result<()> {
        info!(
            "Fetching MEXC data for {}{} via API",
            config.base, config.quote
        );

        let symbol = self.get_symbol(config);
        let start_ms = config.from.timestamp_millis();
        let end_ms = config.to.timestamp_millis();

        // MEXC API has a limit of 1000 trades per request
        // We need to chunk the time range into manageable segments
        // A reasonable chunk size is 1 day (86400000 ms)
        let chunk_duration_ms = 86_400_000; // 1 day
        let mut current_start = start_ms;
        let mut chunk_count = 0;

        let pb = ProgressBar::new(((end_ms - start_ms) / chunk_duration_ms + 1) as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} days ({eta})",
                )
                .unwrap()
                .progress_chars("#>-"),
        );

        while current_start < end_ms {
            let chunk_end = (current_start + chunk_duration_ms).min(end_ms);

            debug!(
                "Fetching MEXC data chunk: {} to {}",
                NaiveDateTime::from_timestamp_millis(current_start)
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S"),
                NaiveDateTime::from_timestamp_millis(chunk_end)
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
            );

            match self
                .fetch_agg_trades(&symbol, current_start, chunk_end)
                .await
            {
                Ok(trades) => {
                    if !trades.is_empty() {
                        let ticks = Self::agg_trades_to_ticks(trades);

                        // Send ticks in batches to avoid overwhelming the channel
                        for batch in ticks.chunks(10000) {
                            if let Err(e) = tx.send(batch.to_vec()).await {
                                warn!("Failed to send tick batch: {}", e);
                                return Ok(()); // Channel closed, stop processing
                            }
                        }

                        chunk_count += 1;
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to fetch MEXC data for chunk starting at {}: {}",
                        current_start,
                        e
                    );
                }
            }

            pb.inc(1);
            current_start = chunk_end + 1;
        }

        pb.finish_with_message(format!(
            "Processed {} day chunks",
            chunk_count
        ));

        info!(
            "Completed MEXC data fetch for {}{}",
            config.base, config.quote
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agg_trades_to_ticks() {
        let trades = vec![
            MexcAggTrade {
                agg_trade_id: 1,
                first_trade_id: 1,
                last_trade_id: 1,
                price: "100.0".to_string(),
                quantity: "1.0".to_string(),
                transact_time: 1640000000000,
                is_buyer_maker: true, // Market sell
            },
            MexcAggTrade {
                agg_trade_id: 2,
                first_trade_id: 2,
                last_trade_id: 2,
                price: "101.0".to_string(),
                quantity: "2.0".to_string(),
                transact_time: 1640000001000,
                is_buyer_maker: false, // Market buy
            },
            MexcAggTrade {
                agg_trade_id: 3,
                first_trade_id: 3,
                last_trade_id: 3,
                price: "100.5".to_string(),
                quantity: "1.5".to_string(),
                transact_time: 1640000002000,
                is_buyer_maker: true, // Market sell
            },
        ];

        let ticks = MexcSource::agg_trades_to_ticks(trades);

        assert_eq!(ticks.len(), 3);

        // First tick: market sell, price becomes bid
        assert_eq!(ticks[0].timestamp, 1640000000000);
        assert_eq!(ticks[0].bid, 100.0);
        assert!(ticks[0].ask > ticks[0].bid); // ask > bid
        assert_eq!(ticks[0].vbid, 100); // 1.0 * 100.0
        assert_eq!(ticks[0].vask, 0);

        // Second tick: market buy, price becomes ask
        assert_eq!(ticks[1].timestamp, 1640000001000);
        assert!(ticks[1].bid < ticks[1].ask); // bid < ask
        assert_eq!(ticks[1].ask, 101.0);
        assert_eq!(ticks[1].vbid, 0);
        assert_eq!(ticks[1].vask, 202); // 2.0 * 101.0

        // Third tick: market sell, price becomes bid
        assert_eq!(ticks[2].timestamp, 1640000002000);
        assert_eq!(ticks[2].bid, 100.5);
        assert_eq!(ticks[2].vbid, 150); // 1.5 * 100.5
        assert_eq!(ticks[2].vask, 0);
    }

    #[test]
    fn test_agg_trades_to_ticks_empty() {
        let trades: Vec<MexcAggTrade> = vec![];
        let ticks = MexcSource::agg_trades_to_ticks(trades);
        assert!(ticks.is_empty());
    }

    #[test]
    fn test_agg_trades_to_ticks_bid_ask_integrity() {
        let trades = vec![
            // First trade initializes both bid and ask
            MexcAggTrade {
                agg_trade_id: 1,
                first_trade_id: 1,
                last_trade_id: 1,
                price: "50000.0".to_string(),
                quantity: "0.1".to_string(),
                transact_time: 1640000000000,
                is_buyer_maker: false,
            },
            // Subsequent trades should maintain bid < ask
            MexcAggTrade {
                agg_trade_id: 2,
                first_trade_id: 2,
                last_trade_id: 2,
                price: "50010.0".to_string(),
                quantity: "0.1".to_string(),
                transact_time: 1640000001000,
                is_buyer_maker: false,
            },
            MexcAggTrade {
                agg_trade_id: 3,
                first_trade_id: 3,
                last_trade_id: 3,
                price: "50005.0".to_string(),
                quantity: "0.1".to_string(),
                transact_time: 1640000002000,
                is_buyer_maker: true,
            },
        ];

        let ticks = MexcSource::agg_trades_to_ticks(trades);

        // Verify bid < ask for all ticks
        for tick in &ticks {
            assert!(
                tick.bid < tick.ask,
                "bid ({}) should be less than ask ({})",
                tick.bid,
                tick.ask
            );
        }

        // Verify volumes are non-negative
        for tick in &ticks {
            assert!(tick.vbid >= 0);
            assert!(tick.vask >= 0);
        }
    }
}
