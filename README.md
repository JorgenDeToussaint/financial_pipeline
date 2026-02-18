# 🌊 Financial Data Lakehouse Engine

## 🎯 Mission
A resilient, modular data infrastructure designed to eliminate "manual guesswork" in financial analysis. 

This project is built as a scalable **"Data Hydraulics"** system, prioritizing:
- Fault tolerance
- Cost-efficiency (FinOps) 
- High-performance analytical processing

**Local-first, Cloud-ready** approach—prototyping on a private cluster of Thinkpad T480 nodes (simulating S3 via MinIO) to ensure 100% architectural compatibility with AWS before deployment.

## 🏗️ Architecture: The Medallion Pattern
The system follows the **Medallion Architecture**, ensuring data integrity at every hop:

| Layer | Description | Format | Purpose |
|-------|-------------|--------|---------|
| **Bronze (Raw)** | Immutable, raw JSON captures from APIs (CoinGecko, NBP, Yahoo Finance) | JSON | Source of truth, audit trail |
| **Silver (Cleansed)** | Schema-enforced, typed, filtered data with circuit breaker logic | Parquet | Optimized using Polars |
| **Gold (Curated)** | Analytical datasets with joins, currency normalization, feature engineering | Optimized for DuckDB | "Data Whiskey" matures here |

## ⚙️ Core Engineering Pillars

1. **Metadata-Driven Scaling**  
   Completely decoupled from data sources. Add new instruments by editing `config/pipes.yaml`—**no Python code changes required**.

2. **Strict Schema Validation (Pydantic)**  
   "Fail-Fast" validation ensures 100% uptime. Pipeline refuses to start if YAML config has typos or invalid parameters.

3. **Factory Pattern Orchestration**  
   Centralized `PipeFactory` handles Extractors and Transformers. Adding new providers (Steam, Binance) = drop in new class.

## 🛠️ Tech Stack & Trade-offs

| Technology | Why Chosen |
|------------|------------|
| **Python (Polars/DuckDB)** | Memory efficiency, lazy evaluation over Pandas—critical for edge computing |
| **Pydantic** | Configuration enforcement, type-safe environment management |
| **MinIO** | Local S3-compatible storage for production-grade local infrastructure |
| **Docker** | "It works on my machine" = "It works on the cluster" reliability |

## 🚀 Quick Start

1. Ensure Docker Engine is running
2. Setup your `.env` file (S3 credentials)
3. Run: 

```bash
docker-compose up --build
