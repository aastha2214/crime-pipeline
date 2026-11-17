"""Load: upsert into PostgreSQL inside ONE transaction, and flag records that vanished from the source."""
import config  # noqa: F401  (must be first)

import os
from typing import Optional

import pandas as pd
import psycopg2
from prefect import task
from prefect.tasks import exponential_backoff
from psycopg2.extras import execute_values

from config import db_params, get_logger

TABLE = "cpd_incidents"

# Columns we write from the pipeline. Order matters: it matches the DataFrame columns we pass to execute_values.
DATA_COLUMNS = [
    "id", "date_time", "type", "subtype", "location", "description", "last_updated",
    "year", "month", "day", "hour", "minute", "second",
]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id               BIGINT PRIMARY KEY,
    date_time        TIMESTAMP NOT NULL,
    type             TEXT NOT NULL,
    subtype          TEXT,
    location         TEXT NOT NULL,
    description      TEXT,
    last_updated     TIMESTAMP NOT NULL,
    year             INTEGER,
    month            INTEGER,
    day              INTEGER,
    hour             INTEGER,
    minute           INTEGER,
    second           INTEGER,
    deleted          BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at       TIMESTAMPTZ,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_in_db_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_{TABLE}_last_updated ON {TABLE} (last_updated);
"""

# Insert new rows; if the id exists, update it ONLY when the source's last_updated changed or the row
# had been flagged deleted (it reappeared). Un-deleting clears the flag. Because this is keyed on id,
# running the same load twice can never create duplicates.
_update_cols = [c for c in DATA_COLUMNS if c != "id"]
UPSERT_SQL = f"""
INSERT INTO {TABLE} ({", ".join(DATA_COLUMNS)}) VALUES %s
ON CONFLICT (id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in _update_cols)},
    deleted = FALSE, deleted_at = NULL, updated_in_db_at = now()
WHERE {TABLE}.last_updated IS DISTINCT FROM EXCLUDED.last_updated OR {TABLE}.deleted
RETURNING (xmax = 0) AS inserted
"""
# xmax = 0 is true only for freshly inserted rows, which lets us count inserts vs updates.


class LoadError(RuntimeError):
    pass


def connect():
    params = db_params()
    if not params["user"] or not params["password"]:
        raise LoadError("POSTGRES_USER / POSTGRES_PASSWORD are not set in .env.")
    return psycopg2.connect(**params)


def create_postgres_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)


def _retry_db_down(task, task_run, state) -> bool:
    return isinstance(state.result(raise_on_failure=False), psycopg2.OperationalError)


@task(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=5), retry_condition_fn=_retry_db_down)
def read_db_state() -> dict:
    """What we already have: the watermark (latest last_updated), plus active and deleted id sets."""
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            create_postgres_table(conn)
            # active rows only: a deleted/bogus row must never push the watermark past what the source has
            cur.execute(f"SELECT max(last_updated) FROM {TABLE} WHERE NOT deleted")
            watermark = cur.fetchone()[0]
            cur.execute(f"SELECT id, deleted FROM {TABLE}")
            rows = cur.fetchall()
    finally:
        conn.close()
    state = {
        "watermark": watermark,  # None on the first run -> full load
        "active_ids": {i for i, d in rows if not d},
        "deleted_ids": {i for i, d in rows if d},
    }
    get_logger().info(
        "DB state: watermark=%s, %d active, %d deleted.", watermark, len(state["active_ids"]), len(state["deleted_ids"])
    )
    return state


@task(retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=5), retry_condition_fn=_retry_db_down)
def load_into_postgres(df: pd.DataFrame, source_ids: Optional[set] = None) -> dict:
    """Upsert `df`, then flag as deleted any active row whose id is no longer in `source_ids`.

    Everything happens in a single transaction: either the whole run lands or nothing does.
    Pass source_ids=None to skip delete detection.
    """
    log = get_logger()
    conn = connect()
    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "restored": 0, "deleted": 0}
    try:
        with conn, conn.cursor() as cur:  # `with conn` = commit on success, rollback on any exception
            create_postgres_table(conn)

            if not df.empty:
                ids = [int(i) for i in df["id"]]
                cur.execute(f"SELECT count(*) FROM {TABLE} WHERE deleted AND id = ANY(%s)", (ids,))
                stats["restored"] = cur.fetchone()[0]
                records = list(df[DATA_COLUMNS].itertuples(index=False, name=None))
                returned = execute_values(cur, UPSERT_SQL, records, page_size=1000, fetch=True)
                stats["inserted"] = sum(1 for (ins,) in returned if ins)
                stats["updated"] = len(returned) - stats["inserted"]
                stats["unchanged"] = len(records) - len(returned)

            if source_ids is not None:
                keep = set(source_ids) | {int(i) for i in df["id"]} if not df.empty else set(source_ids)
                cur.execute(f"SELECT count(*) FROM {TABLE} WHERE NOT deleted")
                active = cur.fetchone()[0]
                cur.execute(f"SELECT id FROM {TABLE} WHERE NOT deleted AND id <> ALL(%s)", (list(keep),))
                gone = [r[0] for r in cur.fetchall()]
                _check_delete_safety(len(keep), len(gone), active)
                if gone:
                    cur.execute(
                        f"UPDATE {TABLE} SET deleted = TRUE, deleted_at = now(), updated_in_db_at = now() "
                        "WHERE id = ANY(%s) AND NOT deleted",
                        (gone,),
                    )
                    stats["deleted"] = cur.rowcount
                    log.warning("Flagged %d records as deleted (no longer in source): %s", len(gone), gone[:10])
        log.info("Load committed: %s", stats)
        return stats
    except Exception:
        log.error("Load failed; transaction rolled back, database unchanged.")
        raise
    finally:
        conn.close()


def _check_delete_safety(n_source: int, n_gone: int, n_active: int):
    """Refuse to mass-delete when the id list looks truncated (an API hiccup, not a real purge)."""
    if n_source == 0 and n_active > 0:
        raise LoadError("Source returned zero ids but the DB has active rows; refusing to flag everything deleted.")
    max_fraction = float(os.getenv("MAX_DELETE_FRACTION", "0.5"))
    if n_active >= 20 and n_gone > max_fraction * n_active:
        raise LoadError(
            f"Would flag {n_gone} of {n_active} active rows deleted (> {max_fraction:.0%}); "
            "looks like a truncated source response. Aborting without changes."
        )
