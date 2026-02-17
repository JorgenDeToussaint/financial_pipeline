import polars as pl
from base import BaseTransformer

class NBPTransformer(BaseTransformer):
    def __init__(self):
        super().__init__("NBP")

    def run_logic(self, data: list) -> pl.DataFrame:
        try:
            if not data or not isinstance(data, list):
                self.logger.warning("Invalid NBP payload format")
                return pl.DataFrame
            
            payload = data[0]
            effective_date = payload.get("effectiveDate")
            rates = payload.get("rates", [])

            if not rates:
                self.logger.warning("No rates found in NBP payload.")
                return pl.DataFrame()
            
            df_clean = (
                pl.DataFrame(rates)
                .with_columns([
                    pl.lit(effective_date).str.to_date("%Y-%m-%d").alias("date"),
                    pl.col("mid").cast(pl.Decimal(2,8))
                ])
                .select(["date", "code", "mid"])
            )

            return df_clean
        
        except Exception as e:
            self.logger.error(f"NBP Parsing failed: {e}")
            return pl.DataFrame