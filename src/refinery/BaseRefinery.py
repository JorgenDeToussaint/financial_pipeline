from abc import ABC, abstractmethod
import duckdb
import os
from datetime import datetime
from src.utils.logger import get_logger

class BaseRefinery(ABC):
    def __init__(self, name: str, s3_client):
        self.name = name
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
        path = f"data/gold/report={self.name}/year={date.year}/month={date.month:02d}/day={date.day:02d}/"
        os.makedirs(path, exist_ok=True)
        file_path = f"{path}data_{date.strftime('%H%M')}.parquet"
        df.write_parquet(file_path)
        self.logger.info(f"💾 Wynik zapisany w: {file_path}")