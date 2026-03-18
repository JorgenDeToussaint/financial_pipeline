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
            return False

        return True

    def transform(self, raw_data: any) -> bytes | None:
        if raw_data is None:
            self.logger.error("Empty payload recieved")
            return None

        try:
            if isinstance(raw_data, (bytes, str)):
                data = json.loads(raw_data)
            else:
                data = raw_data

            df = self.run_logic(data)

            if not self.validate(df):
                return None

            buffer = io.BytesIO()
            df.write_parquet(file=buffer)
            return buffer.getvalue()

        except Exception as e:
            self.logger.error(f"❌ Transformation failed for {self.name}: {e}")
            return None
