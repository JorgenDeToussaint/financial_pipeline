from abc import ABC, abstractmethod
import duckdb
import io
from datetime import datetime
from src.utils.logger import get_logger

class BaseRefinery(ABC):
    def __init__(self, name: str, config, s3_client):
        self.name = name
        self.config = config
        self.s3 = s3_client
        self.logger = get_logger(f"refinery.{name}")

    def run(self, date: datetime):
        try:
            self._verify_dependencies(date)

            with duckdb.connect(":memory:") as conn:
                self.logger.info(f"💎 Start rafinacji raportu: {self.name}")

                conn.execute("INSTALL httpfs; LOAD httpfs;")

                df = self.transform(conn, date)

                self._save_to_gold(df, date)

            self.logger.info(f"✅ Raport {self.name} ukończony pomyślnie.")

        except Exception as e:
            self.logger.error(f"❌ {self.name} failed: {str(e)}")
            raise

    @abstractmethod
    def _verify_dependencies(self, date: datetime):
        #individual circut breaker
        pass

    @abstractmethod
    def transform(self, conn, date: datetime):
        #There goes SQL querry
        pass

    def _save_to_gold(self, df, date: datetime):
# W _save_to_gold — stała nazwa:
        path = f"report={self.name}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data_daily.parquet"    
        buffer = io.BytesIO()
        df.write_parquet(buffer)
    
        self.s3.save(
            data=buffer.getvalue(),
            bucket="gold",
            path=path
        )
        self.logger.info(f"💾 Gold saved: gold/{path}")