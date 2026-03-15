from pydantic import BaseModel
from typing import List, Dict, Any, Literal


class PipeConfig(BaseModel):
    id: str
    extractor_type: Literal["gecko", "nbp", "yahoo"]
    transformer_type: Literal["gecko", "nbp", "yahoo"]
    params: Dict[str, Any] = {}
    granularity: Literal["daily", "hourly"] = "daily"


class AppConfig(BaseModel):
    pipes: List[PipeConfig]
