import asyncio
import aiohttp
from datetime import datetime
from src.registry import EXTRACTOR_REGISTRY, TRANSFORMER_REGISTRY
from src.factories.pipe_factory import PipeFactory
from src.utils.logger import get_logger
from src.utils.path_manager import PathManager
from src.loaders.S3Loader import S3Loader
from src.refinery.ValuationRefinery import ValuationRefinery

class AsyncManager:
    def __init__(self, app_config):
        self.config = app_config
        self.semaphore = asyncio.Semaphore(3)
        self.logger = get_logger("AsyncManager")
        self.s3 = S3Loader()

    async def _process_pipe(self, session: aiohttp.ClientSession, pipe, now: datetime):
        async with self.semaphore:
            try:
                extractor_cls = EXTRACTOR_REGISTRY.get(pipe.extractor_type)
                if not extractor_cls:
                    self.logger.error(f"Unknown extractor: {pipe.extractor_type}")
                    return

                mapping = {
                    "categories": "category",
                    "tickers": "ticker",
                    "tables": "table",
                    "ids": "ids"
                }

                params = pipe.params.copy()
                loop_key = next((k for k in mapping.keys() if k in params), None)
                items = params.pop(loop_key, [None]) if loop_key else [None]
                singular_key = mapping.get(loop_key)

                for item in items:
                    current_params = params.copy()

                    if item:
                        current_params[singular_key] = item
                        display_id = f"{pipe.id}_{item}"
                    else:
                        display_id = pipe.id

                    self.logger.info(f"🚀 [Task: {display_id}] Start")

                    if pipe.extractor_type == "gecko":
                        if "vs_currency" not in current_params:
                            current_params["vs_currency"] = "usd"
                        extractor = extractor_cls(params=current_params)
                    else:
                        extractor = extractor_cls(**current_params)

                    transformer = PipeFactory.get_transformer(pipe.transformer_type, current_params)

                    raw_data = await extractor.fetch(session)
                    if not raw_data:
                        continue

                    bronze_path = PathManager.get_path("bronze", display_id, now, "json", pipe.granularity)
                    self.s3.save(data=raw_data, bucket="bronze", path=bronze_path)

                    silver_data = transformer.transform(raw_data)
                    if silver_data is not None:
                        silver_path = PathManager.get_path("silver", display_id, now, "parquet", pipe.granularity)
                        self.s3.save(data=silver_data, bucket="silver", path=silver_path)

                    self.logger.info(f"✅ [Task: {display_id}] Sukces")

            except Exception as e:
                self.logger.error(f"❌ [Pipe: {pipe.id}] Błąd: {e}")

    async def run_all(self):
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        async with aiohttp.ClientSession() as session:
            tasks = [self._process_pipe(session, pipe, now) for pipe in self.config.pipes]
            await asyncio.gather(*tasks, return_exceptions=True)

            self.logger.info(f"🏆 Start Gold Layer Processing for {date_str}")

            try:
                refinery = ValuationRefinery(config=self.config, s3_client=self.s3)
                refinery.run(now)
                self.logger.info("✅ Pipeline End-to-End Success!")
            except Exception as e:
                self.logger.error(f"❌ Gold Layer failed: {e}")