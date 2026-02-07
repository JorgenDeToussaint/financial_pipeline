# 🌊 Financial Data Lakehouse Engine

## 🎯 Mission
A resilient, modular data infrastructure designed to eliminate "manual guesswork" in financial analysis. This project is built as a **scalable "Data Hydraulics" system**, prioritizing fault tolerance, cost-efficiency (FinOps), and high-performance analytical processing.

I approach Data Engineering with a **"Local-first, Cloud-ready"** mindset—prototyping on a private cluster of **Thinkpad T480 nodes** (simulating S3 via MinIO) to ensure 100% architectural compatibility with AWS before deployment.

## 🏗️ Architecture: The Medallion Pattern
The system follows the **Medallion Architecture**, ensuring data integrity at every hop:

* **Bronze (Raw):** Immutable, raw JSON captures from various APIs (CoinGecko, NBP). Served as the "source of truth" and audit trail.
* **Silver (Cleansed):** Schema-enforced Parquet files. Data is typed, filtered, and optimized using **Polars** for multi-threaded performance.
* **Gold (Curated):** Analytical datasets optimized for **DuckDB**. This is where "Data Whiskey" matures—joined instruments, currency normalization, and feature engineering.

## 🛠️ Tech Stack & Trade-offs
* **Python (Polars/DuckDB):** Chosen over Pandas for memory efficiency and lazy evaluation, critical for edge computing on limited hardware.
* **MinIO:** Local S3-compatible storage to maintain a production-grade infrastructure locally.
* **Docker:** Containerized environment for consistent "it works on my machine" and "it works on the cluster" reliability.

## 🚀 Future Roadmap
- [ ] **Orchestration Layer:** Implementing a custom "Pipe Registry" in `main.py` for effortless scaling to 1000+ sources.
- [ ] **Cross-Instrument Analytics:** Joining NBP Forex rates with crypto volatility metrics in the Gold Layer.
- [ ] **Circuit Breaker Logic:** Automated data quality checks preventing "poisoned" data from reaching the Silver layer.
- [ ] **Deployment:** Transitioning to AWS (Lambda + S3 + Athena) once the local cluster stress-tests are completed.