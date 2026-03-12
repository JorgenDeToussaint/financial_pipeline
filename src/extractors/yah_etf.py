from .base import BaseExtractor
from src.registry import register_extractor

@register_extractor("yahoo")
class YahooETF(BaseExtractor):
    def __init__(self, ticker: str, **kwargs):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        super().__init__(name=f"Yahoo-{ticker}", base_url=url)

    def get_params(self) -> dict:
        return {
            "range": "1mo",
            "interval": "1d",
            "includePrePost": "false"
        }
    
    def get_headers(self) -> dict:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36"
        }

    