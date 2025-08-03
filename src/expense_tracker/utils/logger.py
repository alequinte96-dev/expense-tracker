import logging
import os

from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.environ.get("LOGGER_LEVEL") or logging.INFO)
if not LOGGER.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
