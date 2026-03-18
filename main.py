<<<<<<< HEAD
import os
import json
from datetime import datetime
from src.utils.logger import get_logger
from src.loaders.S3Loader import S3Loader
from src.extractors.gecko import GeckoExtractor
from src.transformers.gecko import GeckoTransformer
from src.utils.path_manager import PathManager

# 1. Inicjalizacja loggera przed pętlą i blokiem main
logger = get_logger("Main")

if __name__ == "__main__":
    logger.info("🌊 System Ready. Starting Financial Pipeline...")
    now = datetime.now()
    
    loader = S3Loader(
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=os.getenv("MINIO_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_PASSWORD", "minioadmin")
    )

    # Rejestr rur
    pipes = [
        {
            "id": "gecko_stable",
            "extractor": GeckoExtractor(
                endpoint="/coins/markets", 
                params={"vs_currency": "usd", "category": "stablecoins", "per_page": 100}
            ),
            "transformer": GeckoTransformer()
        }
    ]

    for pipe in pipes:
        p_id = pipe["id"]
        ext = pipe["extractor"]
        trans = pipe["transformer"]

        # 1. BRONZE: Pobranie i zapis surowego JSONa
        logger.info(f"🚀 Processing pipe: {p_id}")
        raw_data = ext.fetch()
        if not raw_data:
            logger.error(f"❌ Failed to fetch data for {p_id}")
            continue

        path_json = PathManager.get_path("raw", p_id, now, "json")
        if loader.save(raw_data, "bronze", path_json):
            logger.info(f"📦 Bronze layer saved: {path_json}")
            
            # 2. SILVER: Transformacja i zapis Parquet
            if loader.exists("bronze", path_json):
                bronze_bytes = loader.load("bronze", path_json)
                silver_parquet = trans.transform(bronze_bytes)
                
                if silver_parquet:
                    path_pq = PathManager.get_path("processed", p_id, now, "parquet")
                    if loader.save(silver_parquet, "silver", path_pq):
                        logger.info(f"✨ Silver layer saved: {path_pq}")
                        logger.info(f"✅ Pipe {p_id} completed successfully.")

    logger.info("🏁 Pipeline finished.")
=======
import asyncio
import yaml
import pathlib
import src.extractors
import src.transformers
from src.models.pipeconfig import AppConfig
from src.async_manager import AsyncManager


async def start():
    config_path = pathlib.Path("config/pipes.yaml")
    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)
    
    app_config = AppConfig(**config_data)
    manager = AsyncManager(app_config)
    
    await manager.run_all()

if __name__ == "__main__":
    asyncio.run(start())
>>>>>>> development
