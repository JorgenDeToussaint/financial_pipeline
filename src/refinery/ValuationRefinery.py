import os
from datetime import datetime, timedelta
from src.refinery.BaseRefinery import BaseRefinery
from src.exceptions.transformers import DataIntegrityError


class ValuationRefinery(BaseRefinery):
    def __init__(self, config, s3_client):
        super().__init__("ValuationRefinery", config, s3_client)
        self.required_sources = ["gecko", "yahoo", "nbp"]

    def _verify_dependencies(self, date: datetime):
        self.logger.info(f"🔍 Audyt warstwy Silver dla daty: {date}")

        y = date.strftime("%Y")
        m = date.strftime("%m")
        d = date.strftime("%d")
        partition_path = f"year={y}/month={m}/day={d}"

        mapping = {
            "categories": "category",
            "tickers": "ticker",
            "tables": "table",
        }

        missing = []
        for pipe in self.config.pipes:
            params = pipe.params.copy()
            loop_key = next((k for k in mapping.keys() if k in params), None)
            items = params.get(loop_key, [None]) if loop_key else [None]

            for item in items:
                display_id = f"{pipe.id}_{item}" if item else pipe.id

                if pipe.granularity == "hourly":
                    hour = date.strftime("%H")
                    time_str = date.strftime("%H00")
                    path = f"instrument={display_id}/{partition_path}/hour={hour}/data_{time_str}.parquet"

                    if not self.s3.exists(bucket="silver", path=path):
                        prev = date - timedelta(hours=1)
                        prev_path = f"instrument={display_id}/year={prev.strftime('%Y')}/month={prev.strftime('%m')}/day={prev.strftime('%d')}/hour={prev.strftime('%H')}/data_{prev.strftime('%H')}00.parquet"
                        if not self.s3.exists(bucket="silver", path=prev_path):
                            self.logger.warning(f"⚠️ Missing: {path}")
                            missing.append(display_id)
                        else:
                            self.logger.debug(f"✅ OK (prev hour): {prev_path}")
                    else:
                        self.logger.debug(f"✅ OK: {path}")
                else:
                    path = (
                        f"instrument={display_id}/{partition_path}/data_daily.parquet"
                    )
                    if not self.s3.exists(bucket="silver", path=path):
                        self.logger.warning(f"⚠️ Missing: {path}")
                        missing.append(display_id)
                    else:
                        self.logger.debug(f"✅ OK: {path}")

        if missing:
            raise DataIntegrityError(f"Brakujące źródła Silver: {', '.join(missing)}")

        return True

    def transform(self, conn, date: datetime):
        conn.execute(f"""
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='{os.getenv("S3_ACCESS_KEY")}';
            SET s3_secret_access_key='{os.getenv("S3_SECRET_KEY")}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

        conn.execute("""
            CREATE VIEW silver_gecko AS
            SELECT * FROM read_parquet('s3://silver/instrument=gecko_market_scan_*/year=*/month=*/day=*/hour=*/*.parquet');

            CREATE VIEW silver_yahoo AS
            SELECT * FROM read_parquet('s3://silver/instrument=yahoo_finance_primary_*/year=*/month=*/day=*/hour=*/*.parquet');

            CREATE VIEW silver_nbp AS
            SELECT *, date::TIMESTAMP AS datetime
            FROM read_parquet('s3://silver/instrument=nbp_exchange_rates_*/year=*/month=*/day=*/*.parquet');
        """)

        query = """
        WITH consolidated_prices AS (
            SELECT datetime, ticker, 'gecko' AS source,
                   price_usd, total_volume AS volume, market_cap
            FROM silver_gecko
            UNION ALL
            SELECT datetime, ticker, 'yahoo' AS source,
                   close AS price_usd, volume, NULL AS market_cap
            FROM silver_yahoo
        )
        SELECT
            p.*,
            f.mid AS nbp_rate,
            p.price_usd * f.mid AS price_pln
        FROM consolidated_prices p
        ASOF LEFT JOIN silver_nbp f
            ON p.datetime >= f.datetime
        """

        try:
            self.logger.info("🚀 DuckDB: Rozpoczynam transformację Gold...")
            df = conn.execute(query).pl()
            self.logger.info(f"✅ Warstwa Gold gotowa: {len(df)} rekordów.")
            return df
        except Exception as e:
            self.logger.error(f"❌ Transformacja Gold nieudana: {e}")
            raise
