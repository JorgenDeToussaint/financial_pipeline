# Changelog

All notable changes to this project are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### In Progress
- Historical backfill — replay pipeline for date ranges

---

## [1.3.0] — tests-and-refactor — 2026-03-15

### Added
- pytest suite — 18 tests across BaseTransformer, GeckoTransformer, S3Loader
- `tests/transformers/test_base_transformer.py` — validate() and transform() edge cases
- `tests/transformers/test_gecko_transformer.py` — schema, uppercase ticker, invalid payload
- `tests/loaders/test_s3_loader.py` — save/exists/load with mocked boto3
- Streamlit dashboard — price charts, volatility ranking, returns 24h, PLN normalization
- Interactive view selector: macro, defense, commodities, semiconductors, crypto, full universe

### Changed
- Snake_case filenames: `S3Loader.py` → `s3_loader.py`, `BaseRefinery.py` → `base_refinery.py`, `ValuationRefinery.py` → `valuation_refinery.py`, `NBPTrans.py` → `nbp_transformer.py`
- GeckoTransformer i128 overflow fix — extended schema_overrides to all large numeric columns

## [1.2.0] — view-builder — 2026-03-15

### Added
- `ViewBuilder` — config-driven analytical views from Gold OBT
- `config/views.yaml` — declarative view definitions (tickers, source, or all)
- 6 views: `macro_overview`, `defense_tracker`, `commodities`, `semiconductors`, `crypto_defi`, `full_universe`
- Smart Checkpointing in `AsyncManager` — skips Silver fetch if partition exists
- New tickers: defense, semiconductors, shipping, uranium, commodities (40+ instruments)

### Changed
- Gold OBT simplified — window functions moved to ViewBuilder layer
- `_verify_dependencies` tolerates ±1h for rate-limited sources
- Per-item semaphore in `AsyncManager` — better concurrency
- `union_by_name=True` in ViewBuilder — handles schema evolution across Gold partitions

### Fixed
- Gold layer saves to MinIO instead of local filesystem
- `data_daily.parquet` static filename — upsert on rerun, no duplicates

## [1.1.0] — gold-layer — 2026-03-14

### Added
- Gold layer — `ValuationRefinery` with DuckDB ASOF JOIN and FX normalization
- One Big Table (OBT) with return_1h, return_24h, volume_ma_24h, volatility_24h
- NBP historical backfill (last 60 trading days)
- Streamlit service in docker-compose (placeholder)
- `gold` bucket provisioned in createbuckets

### Fixed
- BaseRefinery contract: config param missing from __init__
- ValuationRefinery.transform() signature mismatch
- ASOF JOIN: cast NBP date to TIMESTAMP
- Window functions: split into with_returns CTE (nested window error)
- S3 path double-prefix bug in _verify_dependencies
- pl.DataFrame missing () in NBPTrans
- price_usd cast to Float64 in GeckoTransformer
- asyncio removed from requirements.txt

### Changed
- Gold layer saves to MinIO instead of local filesystem
- Gold layer uses static data_daily.parquet filename (upsert on rerun)
- NBPTransformer handles multi-day historical payload

## [1.0.0] — modular-sync — 2025

### Added
- `AsyncManager` — full async pipeline execution with `aiohttp` and `asyncio.Semaphore(3)`
- `EXTRACTOR_REGISTRY` — `@register_extractor` decorator, auto-discovery via `__init__.py` pkgutil scan
- `PipeFactory` — maps YAML config strings to extractor/transformer classes
- `BaseRefinery` (ABC) — Gold layer foundation with `_verify_dependencies()` circuit breaker and DuckDB integration
- `ValuationRefinery` — skeleton for FX-normalized valuation views (in progress)
- `RateLimitError`, `AuthError`, `ServerError`, `DataIntegrityError` — typed exception hierarchy under `ExtractorError`
- `execution_timer` — `@contextmanager` utility for performance logging
- Support for NBP Table B and Yahoo Finance tickers (SPY, BRK-B, 7203.T, GC=F, ^GSPC)
- Hive-partitioned paths (`instrument=x/year=x/month=x/day=x/hour=x`) via `PathManager`
- `BaseLoader` ABC with `save()`, `load()`, `exists()` interface

### Changed
- Pipeline execution migrated from synchronous `main.py` loop to `AsyncManager`
- `PipeConfig` extended with `granularity` field (`daily` / `hourly`)
- `AppConfig` + `PipeConfig` moved to Pydantic models (`src/models/pipeconfig.py`)

---

## [0.3.0] — factory-pipes

### Added
- `PipeFactory` — static factory for extractor and transformer instantiation
- `pipes.yaml` — declarative pipeline config (id, extractor_type, transformer_type, params, granularity)
- `AppConfig` / `PipeConfig` — Pydantic validation for YAML config
- `PathManager` — centralized Hive-partitioned path generation for Bronze and Silver layers
- NBP transformer (`NBPTransformer`) — Table A/B → typed Parquet (date, code, mid)
- Yahoo Finance transformer (`YahooTransformer`) — OHLCV → typed Parquet

### Changed
- `main.py` refactored to iterate over config-driven pipes instead of hardcoded sources

---

## [0.2.0] — docker-infra

### Added
- `docker-compose.yml` — three-service stack: `minio`, `createbuckets`, `app`
- `createbuckets` service — one-shot MinIO bucket provisioner (`bronze`, `silver`)
- `S3Loader` — boto3 wrapper for MinIO/S3 with `save()` and `load()`
- `BaseLoader` ABC
- MinIO readiness probe with retry loop (10 attempts, 3s delay) in `main.py`
- `.env.example` — documented environment variables
- `.dockerignore` — excludes data/, logs/, `__pycache__`, .env
- `RotatingFileHandler` logger (5MB cap, 3 backups) with hierarchical naming

### Fixed
- Pipeline no longer starts before MinIO is ready — retry loop prevents connection errors on cold start

---

## [0.1.0] — mvp-coingecko

### Added
- `BaseExtractor` (ABC) — `fetch()`, `get_params()`, `get_headers()` with `requests` + timeout
- `GeckoExtractor` — CoinGecko `/coins/markets` endpoint, stablecoins category
- `BaseTransformer` (ABC) — `transform()` template method, `validate()`, `run_logic()` hook
- `GeckoTransformer` — raw JSON → typed Polars DataFrame → Parquet bytes
- Bronze layer — raw JSON saved to local filesystem
- Silver layer — schema-enforced Parquet saved to local filesystem
- Basic `get_logger()` with console handler
