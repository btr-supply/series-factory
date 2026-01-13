pub mod binance;
pub mod bybit;
pub mod bitget;
pub mod kucoin;
pub mod okx;
pub mod synthetic;

use crate::types::{Config, DataSource, Tick};
use tokio::sync::mpsc;
use async_trait::async_trait;
use anyhow::Result;

#[async_trait]
pub trait TickSource: Send + Sync {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<Tick>>,
    ) -> Result<()>;
}

pub async fn create_source(source: &DataSource) -> Result<Box<dyn TickSource>> {
    match source {
        DataSource::Exchange(name) => match name.as_str() {
            "binance" => Ok(Box::new(binance::BinanceSource::new())),
            "bybit" => Ok(Box::new(bybit::BybitSource::new())),
            "bitget" => Ok(Box::new(bitget::BitgetSource::new())),
            "kucoin" => Ok(Box::new(kucoin::KuCoinSource::new())),
            "okx" => Ok(Box::new(okx::OKXSource::new())),
            _ => anyhow::bail!("Unsupported exchange: {}", name),
        },
        DataSource::Synthetic(model) => Ok(Box::new(synthetic::SyntheticSource::new(model.clone()))),
    }
}