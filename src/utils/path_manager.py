from src.loaders.base import BaseLoader
from datetime import datetime

<<<<<<< HEAD
class PathManager:
    @staticmethod
    def get_path(layer: str, instrument: str, ts: datetime, ext: str) -> str:
        partition = f"instrument={instrument}/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"
        return f"{layer}/{partition}/data_{ts.strftime('%H%M%S')}.{ext}"

    @staticmethod
    def is_valid_source(loader: BaseLoader, bucket: str, path: str) -> bool:
        """Walidacja istnienia surowca przed transformacją."""
        return loader.exists(bucket, path)
=======

class PathManager:
    _GRANULARITY_CONFIG = {
        "nbp_table_a": "daily",
        "gecko_stable": "hourly",
        "yahoo_finance": "hourly",
    }

    @staticmethod
    def get_path(
        layer: str, instrument: str, date: datetime, ext: str, granularity: str = None
    ) -> str:
        if not granularity:
            granularity = PathManager._GRANULARITY_CONFIG.get(instrument, "daily")

        partition = f"instrument={instrument}/year={date.year}/month={date.month:02d}/day={date.day:02d}"

        if granularity == "hourly":
            partition += f"/hour={date.hour:02d}"
            filename = f"data_{date.hour:02d}00"
        else:
            filename = "data_daily"

        return f"{partition}/{filename}.{ext}"

    @staticmethod
    def is_valid_source(loader: BaseLoader, bucket: str, path: str) -> bool:
        return loader.exists(bucket, path)
>>>>>>> development
