import polars as pl
import io
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Predefying to keep stable use of RAM
GECKO_SCHEMA = {
    "id": pl.Utf8,         
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "current_price": pl.Decimal(precision=20, scale=8), 
    "market_cap": pl.Decimal(precision=20, scale=2),
    "total_volume": pl.Decimal(precision=20, scale=2),
    "last_updated": pl.Utf8
}

def transform_to_silver(raw_json_bytes: bytes):
    if not raw_json_bytes:
        return None
    
    columns_to_keep = ["id", "symbol", "current_price", "market_cap", "total_volume", "last_updated"]

    try:
        df_eager = pl.read_json(
            io.BytesIO(raw_json_bytes),
            schema=GECKO_SCHEMA
        )

        df_final = (
            df_eager.lazy()
            .select(columns_to_keep)
            .with_columns(
                pl.col("last_updated").str.to_datetime()
            )
            .collect()
        )

        buffer = io.BytesIO()
        df_final.write_parquet(buffer, compression="snappy")
        
        return buffer.getvalue()
    
    except Exception as e:
        logger.error(f"❌ Polars error (schema/transform): {e}")
        return None