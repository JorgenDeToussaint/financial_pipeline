from src.loaders.base import BaseLoader
from datetime import datetime

class PathManager:
    @staticmethod
    def get_path(layer: str, instrument: str, ts: datetime, ext: str) -> str:
        partition = f"instrument={instrument}/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"
        return f"{layer}/{partition}/data_{ts.strftime('%H%M%S')}.{ext}"

    @staticmethod
    def is_valid_source(loader: BaseLoader, bucket: str, path: str) -> bool:
        """Walidacja istnienia surowca przed transformacją."""
        return loader.exists(bucket, path)