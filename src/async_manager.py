import asyncio
import aiohttp
from datetime import datetime
from src.extractors.registry import EXTRACTOR_REGISTRY
from src.factories.pipe_factory import PipeFactory
from src.utils.logger import get_logger
from src.utils.path_manager import PathManager
from src.loaders.S3Loader import S3Loader

class AsyncManager:
    def __init__(self, app_config):
        self.config = app_config
        self.semaphore = asyncio.Semaphore(3)
        self.logger = get_logger("AsyncManager")
        self.s3 = S3Loader()

    async def _process_pipe(self, session: aiohttp.ClientSession, pipe, now: datetime):
        async with self.semaphore:
            self.logger.info(f"🚀 Processing pipe: {pipe.id}")
            try:
                extractor_cls = EXTRACTOR_REGISTRY.get(pipe.extractor_type)
                if not extractor_cls:
                    self.logger.error(f"Unknown extractor: {pipe.extractor_type}")
                    return
                
                params = pipe.params.copy()
                items = []
                key_name = ""

                if pipe.extractor_type == "gecko":
                    if "vs_currency" not in params:
                        params["vs_currency"] = "usd"
                    
                    all_ids = params.pop("ids", [])
                    if all_ids:
                        chunks = [all_ids[i:i + 250] for i in range(0, len(all_ids), 250)]
                        items = [",".join(chunk) for chunk in chunks]
                        key_name = "ids"
                    else:
                        items = [None]
                else:
                    items = params.pop("tickers", None) or params.pop("tables", None)
                    key_name = "ticker" if pipe.extractor_type == "yahoo" else "table"
                    if items is None:
                        items = [None]

                for idx, item in enumerate(items):
                    current_params = params.copy()
                    
                    if item:
                        current_params[key_name] = item
                        suffix = f"batch_{idx}" if key_name == "ids" else item
                        display_id = f"{pipe.id}_{suffix}"
                    else:
                        display_id = pipe.id

                    if pipe.extractor_type == "gecko":
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

                    self.logger.info(f"✅ Sub-pipe {display_id} finished.")

            except Exception as e:
                self.logger.error(f"❌ Error in pipe {pipe.id}: {e}")

    async def run_all(self):
        now = datetime.now()
        async with aiohttp.ClientSession() as session:
            tasks = [self._process_pipe(session, pipe, now) for pipe in self.config.pipes]
            await asyncio.gather(*tasks)