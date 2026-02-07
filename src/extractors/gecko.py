import requests
from src.extractors.base import BaseExtractor
from src.utils.logger import get_logger

class GeckoExtractor(BaseExtractor):
    def __init__(self, endpoint: str = "/coins/markets", params: dict = None):
        self.url = f"https://api.coingecko.com/api/v3{endpoint}"
        self.params = params or {}
        self.logger = get_logger("GeckoExtractor")

    def fetch(self) -> dict:
        """Pobiera surowe dane z API CoinGecko."""
        try:
            response = requests.get(self.url, params=self.params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error: {e}")
            return None

    def get_params(self) -> dict:
        """Implementacja wymaganej metody abstrakcyjnej."""
        return self.params