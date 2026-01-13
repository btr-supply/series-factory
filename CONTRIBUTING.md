# Contributing

**Repository**: https://github.com/btr-supply/series-factory

## Setup

```bash
# Clone
git clone https://github.com/btr-supply/series-factory.git
cd series-factory

# Build
cargo build --release

# Test
cargo test
```

## Stack

| Component | Crate |
|-----------|-------|
| Async runtime | `tokio` 1.42 |
| Parallelism | `rayon` 1.10 |
| CLI | `clap` 4.5 |
| Time | `chrono` 0.4 |
| HTTP | `ureq` 2.10 |
| Serialization | `serde` 1.0 |
| CSV | `csv` 1.3 |
| Compression | `zip` 2.2, `flate2` 1.0 |
| Output (optional) | `arrow` 54.0, `parquet` 54.0 |
| Logging | `tracing`, `tracing-subscriber` |
| Random | `rand` 0.8, `rand_distr` 0.4 |
| Errors | `anyhow` 1.0 |

## Code Style

```bash
cargo fmt
cargo clippy -- -D warnings
```

## Tests

- Integration tests: `tests/integration.rs`
- Test synthetic models (no network): `cargo test --test integration -- synthetic`
- Test specific models: `cargo test --test integration -- gbm heston`

## Commit Format

```
[feat] New feature
[fix] Bug fix
[refac] Code refactoring
[docs] Documentation
[ops] CI/CD, dependencies, infrastructure
```

## Commit & Push

```bash
# Stage changes
git add .

# Commit
git commit -m "[feat] Description"

# Push
git push origin branch-name
```

## Adding Features

### New Data Source
1. Add module in `src/sources/`
2. Implement `TickSource` trait in `src/sources/mod.rs`
3. Add exchange name to `create_source()` match
4. Add integration test

### New Generative Model
1. Add variant to `GenerativeModel` enum in `src/types.rs`
2. Implement in `src/sources/synthetic.rs`
3. Add integration test

### New Aggregate Field
1. Update `Aggregate` struct in `src/types.rs`
2. Implement calculation in `src/aggregation/mod.rs`
3. Update Parquet schema in `src/output/mod.rs`
4. Add test
