import polars as pl
from src.registry import register_transformer

from src.transformers.base import BaseTransformer

class GeckoTransformer(BaseTransformer):
    def __init__(self):
        super().__init__("CoinGecko")

    def run_logic(self, data: list) -> pl.DataFrame:
        try:
            if not data or not isinstance(data, list):
                self.logger.warning("Invalid Gecko payload format.")
                return pl.DataFrame() # Mała poprawka: dodaj nawiasy ()
            
            # --- TO JEST KLUCZOWA ZMIANA ---
            # Definiujemy typy dla "problemowych" kolumn zanim Polars zacznie je czytać
            overrides = {
                "market_cap": pl.Float64,
                "total_volume": pl.Float64
            }
            
            df = pl.from_dicts(data, schema_overrides=overrides)

            df_clean = (
                df.select([
                    pl.col("symbol").str.to_uppercase().alias("ticker"),
                    pl.col("current_price").alias("price_usd"),
                    pl.col("market_cap"),
                    pl.col("total_volume"),
                    pl.col("last_updated")
                ])
                .with_columns([
                    pl.col("last_updated").str.to_datetime(time_zone="UTC").alias("datetime"),
                    pl.col("price_usd").cast(pl.Decimal(24, 8)),
                    pl.col("market_cap").cast(pl.Float64),
                    pl.col("total_volume").cast(pl.Float64)
                ])
                .drop("last_updated")
            )

            return df_clean
        
        except Exception as e:
            self.logger.error(f"Gecko Parsing failed: {e}")
            return pl.DataFrame()