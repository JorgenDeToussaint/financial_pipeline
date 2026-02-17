import yaml
import pathlib
from datetime import datetime
from src.models.pipeconfig import AppConfig
from src.factories.pipe_factory import PipeFactory
from src.utils.logger import get_logger
from src.utils.path_manager import PathManager
from src.utils.s3_loader import S3Loader

logger = get_logger("Main")

def run_pipelines():
    config_path = pathlib.Path("config/pipes.yml")
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    try:
        app_config = AppConfig(**raw_config)
    except Exception as e:
        logger.error(f"❌ Błąd w konfiguracji YAML: {e}")
        return
    
    now = datetime.now()
    s3 = S3Loader()

    logger.info("🌊 System Ready. Starting Financial Pipeline...")

    for pipe in app_config.pipes:
        logger.info(f"🚀 Processing pipe: {pipe.id}")

        try:
            extractor = PipeFactory.get_exctractor(pipe.extractor_type, pipe.params)
            transformer = PipeFactory.get_transformer(pipe.transformer_type, pipe.params)

            raw_data = extractor.fetch()
            if not raw_data:
                continue

            bronze_path = PathManager.get_path(
                layer="bronze",
                instrument=pipe.id,
                date=now,
                ext="json"
                granularity=pipe.granularity
            )

            s3.save(bronze_path, raw_data)
            logger.info(f"📦 Bronze layer saved: {bronze_path}")

            silver_data = transformer.transform(raw_data)

            if silver_data:
                silver_path = PathManager.get_path(
                    layer="silver",
                    instrument=pipe.id,
                    date=now,
                    ext="parquet",
                    granularity=pipe.granularity
                )
                s3.save(silver_path, silver_data)
                logger.info(f"💎 Silver layer saved: {silver_path}")

        except Exception as e:
            logger.error(f"❌ Critical error in pipe {pipe.id}: {e}")
            continue

    logger.info("🏁 Pipeline finished.")

if __name__ == "__main__":
    run_pipelines()