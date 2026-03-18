import polars as pl
from src.transformers.base import BaseTransformer
from src.registry import register_transformer


@register_transformer("yahoo")
class YahooTransformer(BaseTransformer):
    def __init__(self):
        super().__init__("Yahoo")

    def run_logic(self, data: dict) -> pl.DataFrame:
        try:
            chart_result = data.get("chart", {}).get("result", [])
            if not chart_result:
                self.logger.warning("Empty result in Yahoo payload.")
                return pl.DataFrame()

            result = chart_result[0]
            meta = result.get("meta", {})
            ticker = meta.get("symbol", "UNKNOWN")

            timestamps = result.get("timestamp", [])
            indicators = result.get("indicators", {}).get("quote", [])

            if not timestamps or not indicators:
                self.logger.warning(f"Missing data for ticker: {ticker}")
                return pl.DataFrame()

            quote = indicators[0]

            df = pl.DataFrame(
                {
                    "timestamp": timestamps,
                    "open": quote.get("open", []),
                    "high": quote.get("high", []),
                    "low": quote.get("low", []),
                    "close": quote.get("close", []),
                    "volume": quote.get("volume", []),
                }
            )

            df_silver = (
                df.with_columns(
                    [
                        pl.from_epoch("timestamp", time_unit="s").alias("datetime"),
                        pl.lit(ticker).alias("ticker"),
                        pl.col("open").cast(pl.Decimal(18, 8)),
                        pl.col("high").cast(pl.Decimal(18, 8)),
                        pl.col("low").cast(pl.Decimal(18, 8)),
                        pl.col("close").cast(pl.Decimal(18, 8)),
                        pl.col("volume").cast(pl.Int64),
                    ]
                )
                .drop_nulls(subset=["close"])
                .select(
                    ["datetime", "ticker", "open", "high", "low", "close", "volume"]
                )
            )

            return df_silver

        except Exception as e:
            self.logger.error(f"Transformation logic failed for Yahoo: {e}")
            return pl.DataFrame()
