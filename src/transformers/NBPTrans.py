import polars as pl
import io
import json
from src.transformers.base import BaseTransformer
from src.utils.logger import get_logger

class YahooTransformer(BaseTransformer):
    def __init__(self):
        super().__init__("Yahoo")

    def run_logic(self, data: dict) -> pl.DataFrame:
        # Tu ląduje pęseta, bo dane Yahoo są w: data['chart']['result'][0]
        result = data.get('chart', {}).get('result', [{}])[0]
        
        timestamps = result.get('timestamp', [])
        indicators = result.get('indicators', {}).get('quote', [{}])[0]
        
        # Złożenie tego w Polarsie (Vertical Ingestion)
        df = pl.DataFrame({
            "timestamp": timestamps,
            "open": indicators.get('open'),
            "high": indicators.get('high'),
            "low": indicators.get('low'),
            "close": indicators.get('close'),
            "volume": indicators.get('volume')
        })
        
        # Konwersja czasu (Unix -> Datetime)
        return df.with_columns(
            pl.from_epoch("timestamp", time_unit="s").alias("datetime")
        )