# Changelog

All notable changes to this project are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### In Progress
- Gold layer — `ValuationRefinery` with DuckDB views and FX normalization
- Smart Checkpointing — replay transform from Bronze without re-fetching API
- `TRANSFORMER_REGISTRY` — auto-discovery analogous to extractors
- pytest suite — `BaseTransformer`, `S3Loader`, `validate()`

---

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
