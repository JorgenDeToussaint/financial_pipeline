from .base import BaseExtractor
from .registry import register_extractor

@register_extractor("nbp")
class NBPExtractor(BaseExtractor):
    def __init__(self, table: str = "A", **kwargs):
        super().__init__(
            name=f"NBP_{table}", 
            base_url=f"http://api.nbp.pl/api/exchangerates/tables/{table}/"
        )
        self.table = table

    def get_params(self) -> dict:
        return {"format": "json"}