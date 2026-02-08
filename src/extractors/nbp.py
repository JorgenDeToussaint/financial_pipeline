from .base import BaseExtractor

class NBPExtractor(BaseExtractor):
    def __init__(self, table: str = "A"):
        super().__init__(
            name="NBP", 
            base_url=f"http://api.nbp.pl/api/exchangerates/tables/{table}/"
        )
        self.table = table

    def get_params(self) -> dict:
        return {"format": "json"}