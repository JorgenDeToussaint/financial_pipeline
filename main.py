import asyncio
import yaml
import pathlib
import os
import sys
import src.extractors
import src.transformers
from src.models.pipeconfig import AppConfig
from src.async_manager import AsyncManager

async def start():
    config_path = pathlib.Path("config/pipes.yaml")
    if not config_path.exists():
        print(f"❌ Config not found: {config_path}")
        sys.exit(1)

    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)
    
    app_config = AppConfig(**config_data)
    manager = AsyncManager(app_config)
    
    await manager.run_all()

if __name__ == "__main__":
    try:
        asyncio.run(start())
        sys.exit(0)
    except Exception as e:
        print(f"💥 Pipeline crashed: {e}")
        sys.exit(1)