# salesflow-data-pipeline/logger.py
import logging
import os

from rich.logging import RichHandler


def get_logger(log_level="INFO"):
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", log_level),
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler(rich_tracebacks=True)],
        )

    import inspect

    caller_frame = inspect.stack()[1]
    module_name = caller_frame.frame.f_globals["__name__"]

    return logging.getLogger(module_name)
