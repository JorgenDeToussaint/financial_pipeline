import os
import io
import yaml
import duckdb
from src.utils.logger import get_logger
from src.loaders.s3_loader import S3Loader


class ViewBuilder:
    def __init__(self, s3_client: S3Loader, config_path: str = "config/views.yaml"):
        self.s3 = s3_client
        self.logger = get_logger("refinery.ViewBuilder")
        with open(config_path) as f:
            self.views = yaml.safe_load(f)["views"]

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(":memory:")
        conn.execute(f"""
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='{os.getenv("S3_ACCESS_KEY")}';
            SET s3_secret_access_key='{os.getenv("S3_SECRET_KEY")}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            INSTALL httpfs;
            LOAD httpfs;
        """)
        conn.execute("""
            CREATE VIEW gold_obt AS
            SELECT * FROM read_parquet(
                's3://gold/report=ValuationRefinery/year=*/month=*/day=*/data_daily.parquet'
            );
        """)
        return conn

    def _build_filter(self, view: dict) -> str:
        filter_type = view.get("filter_type", "all")
        if filter_type == "tickers":
            tickers = ", ".join(f"'{t}'" for t in view["tickers"])
            return f"ticker IN ({tickers})"
        elif filter_type == "source":
            return f"source = '{view['source']}'"
        return "1=1"

    def _save_view(self, df, view_id: str):
        path = f"views/{view_id}/data_daily.parquet"
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        self.s3.save(data=buffer.getvalue(), bucket="gold", path=path)
        self.logger.info(f"💎 View saved: gold/{path} ({len(df)} rows)")

    def build_all(self):
        self.logger.info("🔨 ViewBuilder: Building all views...")
        conn = self._get_conn()

        for view in self.views:
            try:
                where = self._build_filter(view)
                df = conn.execute(f"""
                    WITH base AS (
                        SELECT *,
                            (price_usd / LAG(price_usd, 1) OVER (
                                PARTITION BY ticker ORDER BY datetime
                            )) - 1 AS return_1h,
                            (price_usd / LAG(price_usd, 24) OVER (
                                PARTITION BY ticker ORDER BY datetime
                            )) - 1 AS return_24h
                        FROM gold_obt
                        WHERE {where}
                    )
                    SELECT *,
                        AVG(volume) OVER (
                            PARTITION BY ticker ORDER BY datetime
                            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                        ) AS volume_ma_24h,
                        STDDEV(return_1h) OVER (
                            PARTITION BY ticker ORDER BY datetime
                            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                        ) AS volatility_24h
                    FROM base
                    ORDER BY ticker, datetime
                """).pl()
                self._save_view(df, view["id"])
                self.logger.info(f"✅ View {view['id']}: {len(df)} rows")
            except Exception as e:
                self.logger.error(f"❌ View {view['id']} failed: {e}")

        conn.close()
        self.logger.info("✅ ViewBuilder: All views complete.")
