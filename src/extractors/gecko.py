from src.extractors.base import BaseExtractor
from .registry import register_extractor

@register_extractor("gecko")
class GeckoExtractor(BaseExtractor):
    def __init__(self, endpoint: str = "/coins/markets", params: dict = None, **kwargs):
        super().__init__(
            name="gecko",
            base_url=f"https://api.coingecko.com/api/v3{endpoint}"
        )
        self.params = params or {}

    def get_params(self) -> dict:
        return self.params