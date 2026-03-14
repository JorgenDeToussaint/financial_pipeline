from .base import BaseExtractor
from src.registry import register_extractor

@register_extractor("nbp")
class NBPExtractor(BaseExtractor):
    def __init__(self, table: str = "A", last: int = None, **kwargs):
        endpoint = f"/api/exchangerates/tables/{table}/last/{last}/" if last else f"/api/exchangerates/tables/{table}/"
        super().__init__(
            name=f"NBP_{table}",
            base_url=f"http://api.nbp.pl{endpoint}"
        )
        self.table = table

    def get_params(self) -> dict:
        return {"format": "json"}