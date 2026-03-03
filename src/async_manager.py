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
                # 1. Pobranie klasy z rejestru
                extractor_cls = EXTRACTOR_REGISTRY.get(pipe.extractor_type)
                if not extractor_cls:
                    self.logger.error(f"Unknown extractor: {pipe.extractor_type}")
                    return
                
                # 2. Obsługa parametrów i pluralizacji (tickers/tables)
                params = pipe.params.copy()
                items = params.pop("tickers", None) or params.pop("tables", None)
                
                if items is None:
                    items = [None]
                
                # 3. Pętla wewnątrz semafora dla każdego instrumentu
                for item in items:
                    current_params = params.copy()

                    if item:
                        key = "ticker" if pipe.extractor_type == "yahoo" else "table"
                        current_params[key] = item
                        display_id = f"{pipe.id}_{item}"
                    else:
                        display_id = pipe.id

                    # 4. Inicjalizacja z poprawnymi parametrami (current_params zamiast pipe.params)
                    extractor = extractor_cls(**current_params)
                    transformer = PipeFactory.get_transformer(pipe.transformer_type, current_params)

                    # --- EKSTRAKCJA (Async) ---
                    raw_data = await extractor.fetch(session)
                    if not raw_data:
                        
                        continue
                
                    # --- WARSTWA BRONZE ---
                    bronze_path = PathManager.get_path("bronze", display_id, now, "json", pipe.granularity)
                    self.s3.save(
                        data=raw_data,
                        bucket="bronze",
                        path=bronze_path
                    )
                    self.logger.info(f"📦 Bronze saved: {bronze_path}")

                    silver_data = transformer.transform(raw_data)

                    # --- WARSTWA SILVER ---
                    if silver_data is not None:
                        
                        silver_path = PathManager.get_path("silver", display_id, now, "parquet", pipe.granularity)
                        self.s3.save(
                            data=silver_data,
                            bucket="silver",
                            path=silver_path
                        )
                        self.logger.info(f"💎 Silver saved: {silver_path}")

                    self.logger.info(f"✅ Finished processing: {display_id}")

            except Exception as e:
                self.logger.error(f"❌ Error in pipe {pipe.id}: {e}")

    async def run_all(self):
        now = datetime.now()
        async with aiohttp.ClientSession() as session:
            tasks = [self._process_pipe(session, pipe, now) for pipe in self.config.pipes]
            await asyncio.gather(*tasks)