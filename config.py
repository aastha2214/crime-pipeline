"""Shared settings and helpers. Import this module FIRST in any module that imports prefect."""
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent

# Keep Prefect's local state (sqlite db, logs) inside the project instead of ~/.prefect.
# Must be set before prefect is imported anywhere.
os.environ.setdefault("PREFECT_HOME", str(PROJECT_DIR / ".prefect"))

load_dotenv(PROJECT_DIR / ".env")

SOCRATA_DOMAIN = os.getenv("SOCRATA_DOMAIN", "data.cambridgema.gov")
DATASET_ID = "3gki-wyrb"  # Cambridge Police Daily Log

# Columns the source dataset is expected to expose. Anything else is a schema change.
EXPECTED_COLUMNS = ["date_time", "id", "type", "subtype", "location", "last_updated", "description"]
REQUIRED_COLUMNS = ["date_time", "id", "type", "location", "last_updated"]


def db_params() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB", "cpd_db"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "connect_timeout": 10,
    }


def get_logger(name: str = "cpd_etl") -> logging.Logger:
    """Prefect run logger when inside a flow/task (shows in the UI), plain logger otherwise (tests, scripts)."""
    try:
        from prefect import get_run_logger

        return get_run_logger()
    except Exception:
        log = logging.getLogger(name)
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        return log
