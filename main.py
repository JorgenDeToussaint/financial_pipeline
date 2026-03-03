import asyncio
import yaml
import pathlib
from src.models.pipeconfig import AppConfig
from src.async_manager import AsyncManager
from src.utils.logger import get_logger

logger = get_logger("Main")

async def main():
    config_path = pathlib.Path("config/pipes.yaml")
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

        app_config = AppConfig(**raw_config)
        manager = AsyncManager(app_config)

        await manager.run_all()


if __name__ == "__main__":
    asyncio.run(main())