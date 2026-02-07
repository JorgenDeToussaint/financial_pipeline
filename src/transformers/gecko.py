import polars as pl
import json
import io
from datetime import datetime
from src.transformers.base import BaseTransformer
from src.utils.logger import get_logger

class GeckoTransformer(BaseTransformer):
    def __init__(self):
        self.logger = get_logger("GeckoTransformer")

    def transform(self, raw_bytes: bytes) -> bytes:
        if not raw_bytes:
            self.logger.error("Empty payload received.")
            return None

        try:
            data = json.loads(raw_bytes)
            
            # Polars Polymorphism: dict (Scalar) vs list (Vector)
            if isinstance(data, dict):
                df = pl.DataFrame([data])
            elif isinstance(data, list):
                df = pl.from_dicts(data)
            else:
                raise TypeError(f"Unexpected data format: {type(data)}")

            # Metadata Injection
            df = df.with_columns([
                pl.lit(datetime.now().isoformat()).alias("processed_at"),
                pl.lit("coingecko").alias("data_source")
            ])

            # Memory-efficient Parquet serialization
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            return buffer.getvalue()

        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            return None