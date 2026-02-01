import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

def get_logger(name: str):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger(name)

    if logger.hasHandlers():
        return logger
    
    formatter = logging.Formatter(
        "%(asctime)s" | "%(levelname)-8s"| "%(name)s" | "%(message)s" ,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_dir / "pipeline.log",
        maxBytes=5*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )

    file_handler.addHandler(logging.DEBUG)
    file_handler.addHandler(console_handler)

    return logger