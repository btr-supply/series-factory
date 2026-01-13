pub mod aggregation;
pub mod display;
pub mod sources;
pub mod types;

// Re-exports for library users
pub use aggregation::{Aggregator, aggregate_multi_source};
pub use display::display_data_table;
pub use sources::{create_source, TickSource};
pub use types::{
    Aggregate, AggTrade, AggregationMode, Config, DataSource, GenerativeModel, Tick,
    WeightMode, normalize_weights, round_to_6_sig_digits,
    DEFAULT_SPREAD, PARALLEL_THRESHOLD, STREAMING_BUFFER_SIZE,
};
