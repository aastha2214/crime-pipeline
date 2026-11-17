"""Extract: pull Cambridge Police Daily Log data from the Socrata API (via sodapy)."""
import config  # noqa: F401  (must be first: sets PREFECT_HOME and loads .env)

import os
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from prefect import task
from prefect.tasks import exponential_backoff
from sodapy import Socrata

from config import DATASET_ID, SOCRATA_DOMAIN, get_logger


class TransientAPIError(Exception):
    """API is down, timing out or rate limiting us. Worth retrying."""


class PermanentAPIError(Exception):
    """API rejected the request (bad token, dataset gone, bad query) or returned junk. Retrying won't help."""


def _is_transient(task, task_run, state) -> bool:
    """Prefect retry_condition_fn: only retry failures that can plausibly fix themselves."""
    exc = state.result(raise_on_failure=False)
    return isinstance(exc, TransientAPIError)


# Retry up to 4 times, waiting ~10s, 20s, 40s, 80s (+/- jitter) so we don't hammer a struggling API.
api_task = task(
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.5,
    retry_condition_fn=_is_transient,
)


def _client() -> Socrata:
    token = os.getenv("SOCRATA_APP_TOKEN") or None
    if token is None:
        get_logger().warning("SOCRATA_APP_TOKEN is not set; requests may be throttled.")
    # 30s timeout instead of the 10s default: full pulls can be slow.
    return Socrata(SOCRATA_DOMAIN, token, timeout=30)


def _translate(exc: Exception) -> Exception:
    """Turn low-level requests errors into errors with a clear message and a retry classification."""
    if isinstance(exc, requests.exceptions.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        if status == 429:
            return TransientAPIError("Socrata rate limit hit (HTTP 429). Is SOCRATA_APP_TOKEN set and valid?")
        if status in (408, 500, 502, 503, 504):
            return TransientAPIError(f"Socrata server error (HTTP {status}); API may be down.")
        if status in (401, 403):
            return PermanentAPIError(f"Socrata rejected our credentials (HTTP {status}). Check SOCRATA_APP_TOKEN.")
        if status == 404:
            return PermanentAPIError(f"Dataset {DATASET_ID} not found on {SOCRATA_DOMAIN} (HTTP 404).")
        return PermanentAPIError(f"Socrata request failed (HTTP {status}): {exc}")
    if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return TransientAPIError(f"Could not reach {SOCRATA_DOMAIN} ({type(exc).__name__}): {exc}")
    return exc


def _fetch_records(**query) -> list:
    """Fetch every row matching a SoQL query, paginating. Verifies the response shape."""
    client = _client()
    try:
        # order by the system row id so pagination (limit/offset) is stable between pages
        records = list(client.get_all(DATASET_ID, order=":id", **query))
    except Exception as exc:
        raise _translate(exc) from exc
    finally:
        client.close()
    if not all(isinstance(r, dict) for r in records):
        raise PermanentAPIError("Socrata returned unexpected data (rows were not JSON objects).")
    return records


@api_task
def fetch_source_schema() -> list:
    """Column names the source dataset currently publishes (from dataset metadata, not from data rows).

    Socrata omits null fields from row JSON, so a column that is null in every fetched row would look
    'removed' if we inferred the schema from the DataFrame. The metadata is the reliable source.
    """
    client = _client()
    try:
        meta = client.get_metadata(DATASET_ID)
    except Exception as exc:
        raise _translate(exc) from exc
    finally:
        client.close()
    try:
        return [c["fieldName"] for c in meta["columns"] if not c["fieldName"].startswith(":")]
    except (KeyError, TypeError) as exc:
        raise PermanentAPIError(f"Dataset metadata has an unexpected shape: {exc!r}") from exc


@api_task
def extract_source_ids() -> set:
    """Every id currently in the source. Used to detect records that disappeared."""
    records = _fetch_records(select="id")
    ids = pd.to_numeric(pd.Series([r.get("id") for r in records], dtype="object"), errors="coerce").dropna()
    result = set(ids.astype("int64"))
    get_logger().info("Source currently holds %d ids.", len(result))
    return result


@api_task
def extract_data(since: Optional[datetime] = None) -> pd.DataFrame:
    """Rows new or modified in the source.

    since=None  -> full load (first run).
    since=<ts>  -> only rows with last_updated >= ts. We use >= (not >) because many rows share one
                   last_updated value; re-fetching the boundary rows is harmless since loads are upserts.
    """
    query = {}
    if since is not None:
        query["where"] = f"last_updated >= '{since.isoformat(timespec='milliseconds')}'"
    records = _fetch_records(**query)
    get_logger().info("Extracted %d rows (%s).", len(records), f"since {since}" if since else "full load")
    return pd.DataFrame.from_records(records)


@api_task
def extract_by_ids(ids: list) -> pd.DataFrame:
    """Fetch specific rows by id (records we're missing or had flagged deleted that are back in the source)."""
    records = []
    ids = sorted(ids)
    for i in range(0, len(ids), 200):  # chunk to keep the query string short
        chunk = ids[i : i + 200]
        in_list = ",".join(f"'{int(x)}'" for x in chunk)  # id is a text column in Socrata
        records.extend(_fetch_records(where=f"id in ({in_list})"))
    get_logger().info("Backfilled %d rows by id.", len(records))
    return pd.DataFrame.from_records(records)
