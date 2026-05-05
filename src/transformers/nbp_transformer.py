import polars as pl
from src.transformers.base import BaseTransformer
from src.registry import register_transformer


@register_transformer("nbp")
def run_logic(self, data: list) -> pl.DataFrame:
    try:
        df = (
            pl.from_dicts(data)
            .explode("rates")
            .unnest("rates")
            .with_columns([
                pl.col("effectiveDate").str.to_date("%Y-%m-%d").alias("date"),
                pl.col("mid").cast(pl.Float64)
            ])
            .select(["date", "code", "mid"])
        )

        if df.is_empty():
            self.logger.warning("NBP Transformer produced empty DataFrame")
            return df

        if df.select(pl.col("mid").null_count()).item() > 0:
            raise ValueError("Detected NULLs in currency rates (mid column)!")

        return df

    except Exception as e:
        self.logger.error(f"NBP Logic Failed: {str(e)}")
        return pl.DataFrame()