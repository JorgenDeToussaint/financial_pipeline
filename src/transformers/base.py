from abc import ABC, abstractmethod
import polars as pl
import io
import json
from src.utils.logger import get_logger


class BaseTransformer(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(f"transformer.{name}")

    @abstractmethod
    def run_logic(self, data: any) -> pl.DataFrame:
        pass

    def validate(self, df: pl.DataFrame) -> bool:
        if df is None or df.height == 0:
            self.logger.error("Validation Failed: DataFrame is empty.")

        null_count = df.null_count().sum().sum()
        if null_count > (df.height * df.width * 0.5):
            self.logger.error(f"❌ Validation Failed: Too many nulls ({null_count})")
            return False
        
        return True

    def transform(self, raw_bytes: bytes) -> bytes | None:
        if not raw_bytes:
            self.logger.error("Empty payload recieved")
            return None
        
        try:
            data = json.loads(raw_bytes)

            df = self.run_logic(data)

            if not self.validate(df):
                return None
            
            buffer = io.BytesIO()
            df.write_parquet(file=buffer)
            return buffer.getvalue()
        
        except Exception as e:
            self.logger.error(f"❌ Transformation failed for {self.name}: {e}")
            return None