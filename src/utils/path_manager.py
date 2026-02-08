from src.loaders.base import BaseLoader
from datetime import datetime

class PathManager:
    
    _GRANULARITY_CONFIG = {
        "nbp_table_a": "daily",    
        "gecko_stable": "hourly",  
        "yahoo_finance": "hourly"  
    }

    @staticmethod
    def get_path(layer: str, instrument: str, ts: datetime, ext: str) -> str:
        granularity = PathManager._GRANULARITY_CONFIG.get(instrument, "daily")
        
        partition = f"instrument={instrument}/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"
        
        if granularity == "hourly":
            partition += f"/hour={ts.hour:02d}"
            filename = f"data_{ts.hour:02d}00" 
            filename = "data_daily"

        return f"{layer}/{partition}/{filename}.{ext}"

    @staticmethod
    def is_valid_source(loader: BaseLoader, bucket: str, path: str) -> bool:
        return loader.exists(bucket, path)