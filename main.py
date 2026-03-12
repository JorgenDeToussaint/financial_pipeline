import asyncio
import yaml
import pathlib
import src.extractors
import src.transformers
from src.models.pipeconfig import AppConfig
from src.async_manager import AsyncManager


async def start():
    config_path = pathlib.Path("config/pipes.yaml")
    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)
    
    app_config = AppConfig(**config_data)
    manager = AsyncManager(app_config)
    
    await manager.run_all()

if __name__ == "__main__":
    asyncio.run(start())