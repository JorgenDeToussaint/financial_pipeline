from .base import BaseExtractor

class YahooETF(BaseExtractor):
    def __init__(self, ticker: str):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        super().__init__(name=f"Yahoo-{ticker}", base_url=url)

    def get_params(self) -> dict:
        return {
            "range": "id",
            "interval": "id",
            "includePrePost": "false"
        }
    
    def get_headers(self) -> dict:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36"
        }

    