# salesflow-data-pipeline/logger.py
import logging
import os

from rich.logging import RichHandler


def configure_logging(log_level: str = "INFO") -> None:
    """Configure the root logger with rich handler."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", log_level),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
        force=True,
    )


def get_logger(name: str | None = None) -> logging.Logger:
    """Get a logger with the given name, or the caller's module name if None."""
    if not logging.getLogger().handlers:
        configure_logging()

    if name is None:
        # Get the caller's module name
        import inspect

        frame = inspect.stack()[1]
        name = frame.frame.f_globals["__name__"]

    return logging.getLogger(name)
