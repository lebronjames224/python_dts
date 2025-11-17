import logging
from typing import Optional

from src.common.config import settings

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

def configure_logging(level: Optional[str] = None) -> None:
    """
    Configure the root logger once so every service (API, scheduler, worker) shares formatting.
    """
    lvl_name = (level or settings.log_level or "INFO").upper()
    lvl_value = getattr(logging, lvl_name, logging.INFO)
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=lvl_value, format=LOG_FORMAT)
    root.setLevel(lvl_value)

def get_logger(name: Optional[str] = None) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name or "python_dts")
