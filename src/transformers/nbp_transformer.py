import polars as pl
from src.transformers.base import BaseTransformer
from src.registry import register_transformer


@register_transformer("nbp")
class NBPTransformer(BaseTransformer):
    def __init__(self):
        super().__init__("NBP")

    def run_logic(self, data: list) -> pl.DataFrame:
        try:
            if not data or not isinstance(data, list):
                self.logger.warning("Invalid NBP payload format")
                return pl.DataFrame()

            frames = []
            for payload in data:
                effective_date = payload.get("effectiveDate")
                rates = payload.get("rates", [])
                if not rates:
                    continue

                df = (
                    pl.DataFrame(rates)
                    .with_columns(
                        [
                            pl.lit(effective_date)
                            .str.to_date("%Y-%m-%d")
                            .alias("date"),
                            pl.col("mid").cast(pl.Float64),
                        ]
                    )
                    .select(["date", "code", "mid"])
                )
                frames.append(df)

            if not frames:
                self.logger.warning("No rates found in NBP payload.")
                return pl.DataFrame()

            return pl.concat(frames)

        except Exception as e:
            self.logger.error(f"NBP Parsing failed: {e}")
        return pl.DataFrame()
