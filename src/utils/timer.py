import time
from contextlib import contextmanager


@contextmanager
def execution_timer(name: str, logger):
    start_time = time.perf_counter()
    yield
    end_time = time.perf_counter()
    duration = end_time - start_time
    logger.info(f"[{name}] time: {duration:.4f}s")
