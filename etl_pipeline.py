"""Prefect flow tying extract -> validate -> transform -> load together, plus the daily schedule.

Usage:
    python etl_pipeline.py                  # serve the flow on its daily schedule (needs a Prefect server)
    python etl_pipeline.py run              # run once now (incremental)
    python etl_pipeline.py run --full-refresh   # run once, ignoring the stored watermark
"""
import config  # noqa: F401  (must be first)

import os
import sys

import pandas as pd
from prefect import flow, get_run_logger

from extract import extract_by_ids, extract_data, extract_source_ids, fetch_source_schema
from load import load_into_postgres, read_db_state
from transform import transform_data
from validate import validate_data


def log_failure(flow, flow_run, state):
    """on_failure hook: one clear line saying which run failed and why."""
    try:
        reason = repr(state.result(raise_on_failure=False))
    except Exception:  # pragma: no cover
        reason = state.message
    get_run_logger().error(
        "ETL run '%s' FAILED: %s. Database was not modified by this run (loads are transactional).",
        flow_run.name, reason,
    )


@flow(name="cpd_incident_etl", log_prints=True, on_failure=[log_failure])
def etl(full_refresh: bool = False):
    """
    - Read what's already in Postgres (latest last_updated = watermark)
    - Extract only rows new/modified since the watermark (everything on the first run)
    - Validate and transform them
    - Upsert into Postgres and flag records that disappeared from the source, in one transaction
    """
    log = get_run_logger()

    log.info("Reading current database state...")
    db = read_db_state()
    since = None if full_refresh else db["watermark"]
    log.info("Mode: %s", f"incremental since {since}" if since else "FULL LOAD")

    log.info("Checking source schema and listing source ids...")
    source_columns = fetch_source_schema()
    source_ids = extract_source_ids()

    log.info("Extracting changed rows...")
    changed = extract_data(since)

    # Self-heal: rows the watermark can't catch (back-dated inserts, or rows we'd flagged deleted that
    # reappeared with an old last_updated). Fetch any source id we lack or have marked deleted.
    changed_ids = set(pd.to_numeric(changed["id"], errors="coerce").dropna().astype("int64")) if not changed.empty else set()
    wanted = (source_ids - db["active_ids"] - db["deleted_ids"]) | (source_ids & db["deleted_ids"])
    wanted -= changed_ids
    frames = [changed]
    if wanted:
        log.info("Backfilling %d ids that are missing or were flagged deleted...", len(wanted))
        frames.append(extract_by_ids(sorted(wanted)))
    frames = [f for f in frames if not f.empty]
    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if full_refresh or since is None:
        if raw.empty:
            raise RuntimeError("Full load fetched zero rows; refusing to continue (API returned no data).")

    log.info("Validating...")
    validated = validate_data(raw, source_columns)

    log.info("Transforming...")
    transformed = transform_data(validated)

    log.info("Loading into Postgres...")
    stats = load_into_postgres(transformed, source_ids)

    log.info("ETL complete: %s", stats)
    return stats


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "run":
        etl(full_refresh="--full-refresh" in args)
    else:
        from prefect.schedules import Cron

        # CPD data is updated daily, so run every day at midnight Cambridge (Eastern) time.
        # ETL_CRON / ETL_TIMEZONE override this (e.g. ETL_CRON="* * * * *" to test the scheduler).
        # NOTE: scheduling needs a running Prefect server (PREFECT_API_URL); on Prefect's temporary in-process
        # server the flow is served but nothing is ever triggered.
        cron = os.getenv("ETL_CRON", "0 0 * * *")
        tz = os.getenv("ETL_TIMEZONE", "America/New_York")
        etl.serve(name="cpd-pipeline-deployment", schedule=Cron(cron, timezone=tz))
