pub mod binance;
pub mod mexc;
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
            "mexc" => Ok(Box::new(mexc::MexcSource::new())),
            _ => anyhow::bail!("Unsupported exchange: {}", name),
        },
        DataSource::Synthetic(model) => Ok(Box::new(synthetic::SyntheticSource::new(model.clone()))),
    }
}