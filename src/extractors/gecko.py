from src.extractors.base import BaseExtractor

class GeckoExtractor(BaseExtractor):
    def __init__(self, pipe_id: str, category: str = None, vs_currency: str = "usd"):
        # pipe_id to unikalna nazwa instancji, np. 'gecko_stables'
        super().__init__(name=pipe_id, base_url="https://api.coingecko.com/api/v3/coins/markets")
        self.category = category
        self.vs_currency = vs_currency

    def get_params(self) -> dict:
        params = {
            "vs_currency": self.vs_currency,
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1
        }
        if self.category:
            params["category"] = self.category
        return params