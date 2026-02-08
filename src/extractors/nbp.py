import requests
from src.extractors.base import BaseExtractor
from src.utils.logger import get_logger

class NBPExtractor(BaseExtractor):
    def __init__(self, table: str = "A"):
        self.logger = get_logger("NBPExtractor")
        self.table = table
        # Endpoint dla całej tabeli (ostatnie notowanie)
        self.url = f"http://api.nbp.pl/api/exchangerates/tables/{self.table}/"

    def fetch(self) -> dict:
        self.logger.info(f"Fetching full NBP Table {self.table}")
        try:
            response = requests.get(self.url, params={"format": "json"}, timeout=15)
            response.raise_for_status()
            return response.json()[0]
        
        except Exception as e:
            self.logger.error(f"Failed to fetch NBP Table: {e}")
            raise

    def get_params(self) -> dict:
        return {"source": "nbp", "table": self.table}