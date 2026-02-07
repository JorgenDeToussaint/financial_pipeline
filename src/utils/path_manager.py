from datetime import datetime

class PathManager:
    @staticmethod
    def get_bronze_path(instrument: str, ts: datetime) -> str:
        """Generuje standard: raw/instrument/rok/miesiąc/dzień/plik.json"""
        return (
            f"raw/instrument={instrument}/"
            f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/"
            f"data_{ts.strftime('%H%M%S')}.json"
        )