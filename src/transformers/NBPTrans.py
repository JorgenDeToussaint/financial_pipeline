import polars as pl
import io
import json
from src.transformers.base import BaseTransformer
from src.utils.logger import get_logger

class NBPTransformer(BaseTransformer):
    def __init__(self):
        self.logger = get_logger("NBPTransformer")

    def transform(self, raw_bytes: bytes) -> bytes:
        self.logger.info("Normalizing NBP Table to Silver")
        data = json.loads(raw_bytes)
        effective_date = data.get('effectiveDate')
        rates = data.get('rates', [])

        df = pl.from_dicts(rates).select([
            pl.lit(effective_date).str.to_date().alias("date"),
            pl.col("code").alias("currency"),
            pl.col("mid").alias("rate_pln")
        ])
        pln_row = pl.DataFrame({"date": [df["date"][0]], "currency": ["PLN"], "rate_pln": [1.0]})
        df = pl.concat([df, pln_row])

        self.logger.info(f"Silver NBP ready. Currencies processed: {df.height}")

        buffer = io.BytesIO()
        df.write_parquet(buffer)
        return buffer.getvalue()