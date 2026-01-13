use crate::types::{Config, GenerativeModel, Tick};
use crate::sources::TickSource;
use anyhow::Result;
use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Exp, Normal, Poisson};
use tokio::sync::mpsc;
use tracing::info;

pub struct SyntheticSource {
    model: GenerativeModel,
}

impl SyntheticSource {
    pub fn new(model: GenerativeModel) -> Self {
        Self { model }
    }

    fn generate_ticks(&self, config: &Config) -> Vec<Tick> {
        let mut rng = StdRng::from_entropy();
        let mut ticks = Vec::new();
        
        // Fixed 500ms epoch for synthetic data generation
        const SYNTHETIC_EPOCH_MS: f64 = 500.0;
        const EPOCHS_PER_YEAR: f64 = (365.25 * 24.0 * 60.0 * 60.0 * 1000.0) / SYNTHETIC_EPOCH_MS; // ~63,115,200 epochs per year
        
        // Calculate number of ticks to generate based on time range
        let duration_ms = (config.to.timestamp_millis() - config.from.timestamp_millis()) as f64;
        let num_ticks = (duration_ms / SYNTHETIC_EPOCH_MS) as usize;
        let mut timestamp = config.from.timestamp_millis();
        
        match &self.model {
            GenerativeModel::GBM { mu, sigma, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();
                
                // Convert yearly parameters to per-epoch
                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.sqrt();
                
                for _ in 0..num_ticks {
                    let z = normal.sample(&mut rng);
                    let drift = mu_per_epoch;
                    let diffusion = sigma_per_epoch * z;
                    
                    price = price * (1.0 + drift + diffusion);
                    
                    let spread = price * 0.0001; // 0.01% spread
                    let is_buy = rng.gen_bool(0.5);
                    
                    ticks.push(Tick {
                        timestamp,
                        bid: price - spread / 2.0,
                        ask: price + spread / 2.0,
                        vbid: if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 },
                        vask: if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 },
                    });
                    
                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            
            GenerativeModel::FBM { mu, sigma, hurst, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();
                let mut prev_increment = 0.0;
                
                // Convert yearly parameters to per-epoch
                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.powf(*hurst);
                
                for i in 0..num_ticks {
                    let z = normal.sample(&mut rng);
                    
                    // Fractional Brownian motion increment
                    let h = *hurst;
                    let correlation = if i > 0 {
                        ((i as f64).powf(2.0 * h) - 2.0 * ((i - 1) as f64).powf(2.0 * h) + 
                         if i > 1 { ((i - 2) as f64).powf(2.0 * h) } else { 0.0 }) / 2.0
                    } else {
                        1.0
                    };
                    
                    let increment = correlation.sqrt() * z + (1.0 - correlation).sqrt() * prev_increment;
                    prev_increment = increment;
                    
                    let drift = mu_per_epoch;
                    let diffusion = sigma_per_epoch * increment;
                    
                    price = price * (1.0 + drift + diffusion);
                    
                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);
                    
                    ticks.push(Tick {
                        timestamp,
                        bid: price - spread / 2.0,
                        ask: price + spread / 2.0,
                        vbid: if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 },
                        vask: if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 },
                    });
                    
                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            
            GenerativeModel::Heston { mu, sigma, kappa, theta, xi, rho, base } => {
                let mut price = *base;
                let mut variance = sigma * sigma;
                let normal = Normal::new(0.0, 1.0).unwrap();
                
                // Convert yearly parameters to per-epoch
                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let kappa_per_epoch = kappa / EPOCHS_PER_YEAR;
                let xi_per_epoch = xi / EPOCHS_PER_YEAR.sqrt();
                let dt_epoch = 1.0 / EPOCHS_PER_YEAR; // Epoch as fraction of year
                
                for _ in 0..num_ticks {
                    let z1 = normal.sample(&mut rng);
                    let z2 = normal.sample(&mut rng);
                    let z2_corr = rho * z1 + (1.0 - rho * rho).sqrt() * z2;
                    
                    // Update variance (CIR process)
                    let d_w_v = dt_epoch.sqrt() * z2_corr;
                    let d_variance = kappa_per_epoch * (theta - variance) * dt_epoch + xi_per_epoch * variance.sqrt() * d_w_v;
                    variance = (variance + d_variance).max(0.0001); // Ensure positive variance
                    
                    // Update price
                    let d_w_s = dt_epoch.sqrt() * z1;
                    let d_log_price = (mu_per_epoch - 0.5 * variance) * dt_epoch + variance.sqrt() * d_w_s;
                    price = price * d_log_price.exp();
                    
                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);
                    
                    ticks.push(Tick {
                        timestamp,
                        bid: price - spread / 2.0,
                        ask: price + spread / 2.0,
                        vbid: if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 },
                        vask: if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 },
                    });
                    
                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            
            GenerativeModel::NormalJumpDiffusion { mu, sigma, lambda, mu_jump, sigma_jump, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();
                let jump_normal = Normal::new(*mu_jump, *sigma_jump).unwrap();
                
                // Convert yearly parameters to per-epoch
                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.sqrt();
                let lambda_per_epoch = lambda / EPOCHS_PER_YEAR; // Jump frequency per epoch
                let poisson = Poisson::new(lambda_per_epoch).unwrap();
                
                for _ in 0..num_ticks {
                    let z = normal.sample(&mut rng);
                    
                    // Diffusion component
                    let drift = mu_per_epoch;
                    let diffusion = sigma_per_epoch * z;
                    
                    // Jump component
                    let num_jumps = poisson.sample(&mut rng) as i32;
                    let mut jump_component = 0.0;
                    for _ in 0..num_jumps {
                        jump_component += jump_normal.sample(&mut rng);
                    }
                    
                    price = price * (1.0 + drift + diffusion + jump_component);
                    
                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);
                    
                    ticks.push(Tick {
                        timestamp,
                        bid: price - spread / 2.0,
                        ask: price + spread / 2.0,
                        vbid: if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 },
                        vask: if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 },
                    });
                    
                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            
            GenerativeModel::DoubleExpJumpDiffusion { mu, sigma, lambda, mu_pos_jump, mu_neg_jump, p_neg_jump, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();
                
                // Convert yearly parameters to per-epoch
                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.sqrt();
                let lambda_per_epoch = lambda / EPOCHS_PER_YEAR; // Jump frequency per epoch
                let poisson = Poisson::new(lambda_per_epoch).unwrap();
                let exp_pos = Exp::new(1.0 / mu_pos_jump).unwrap();
                let exp_neg = Exp::new(1.0 / mu_neg_jump.abs()).unwrap();
                
                for _ in 0..num_ticks {
                    let z = normal.sample(&mut rng);
                    
                    // Diffusion component
                    let drift = mu_per_epoch;
                    let diffusion = sigma_per_epoch * z;
                    
                    // Jump component
                    let num_jumps = poisson.sample(&mut rng) as i32;
                    let mut jump_component = 0.0;
                    for _ in 0..num_jumps {
                        if rng.gen::<f64>() < *p_neg_jump {
                            // Negative jump
                            jump_component -= exp_neg.sample(&mut rng);
                        } else {
                            // Positive jump
                            jump_component += exp_pos.sample(&mut rng);
                        }
                    }
                    
                    price = price * (1.0 + drift + diffusion + jump_component);
                    
                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);
                    
                    ticks.push(Tick {
                        timestamp,
                        bid: price - spread / 2.0,
                        ask: price + spread / 2.0,
                        vbid: if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 },
                        vask: if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 },
                    });
                    
                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
        }
        
        ticks
    }
}

#[async_trait]
impl TickSource for SyntheticSource {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<Tick>>,
    ) -> Result<()> {
        info!("Generating synthetic data using {:?}", self.model);

        let ticks = self.generate_ticks(config);
        let total_ticks = ticks.len();

        // Send in batches
        for chunk in ticks.chunks(10000) {
            tx.send(chunk.to_vec()).await?;
        }

        info!("Generated {} synthetic ticks", total_ticks);
        Ok(())
    }

}