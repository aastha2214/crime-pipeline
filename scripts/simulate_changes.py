"""Helpers to simulate source changes against the local DB, then re-run the pipeline to see it react.

    python scripts/simulate_changes.py report          # counts, duplicates, deleted rows
    python scripts/simulate_changes.py edit            # make DB look stale for the most recently updated record
    python scripts/simulate_changes.py remove          # add a fake record that does not exist in the source
    python scripts/simulate_changes.py show <id>       # print one row
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from load import connect  # noqa: E402

FAKE_ID = 999999999


def q(cur, sql, args=None):
    cur.execute(sql, args)
    return cur.fetchall()


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "report"
    conn = connect()
    with conn, conn.cursor() as cur:
        if cmd == "report":
            total, distinct, active, deleted, wm = q(
                cur, "SELECT count(*), count(DISTINCT id), count(*) FILTER (WHERE NOT deleted), "
                     "count(*) FILTER (WHERE deleted), max(last_updated) FROM cpd_incidents")[0]
            print(f"rows={total} distinct_ids={distinct} duplicates={total - distinct} "
                  f"active={active} deleted={deleted} watermark={wm}")
            for r in q(cur, "SELECT id, deleted, deleted_at, location FROM cpd_incidents WHERE deleted ORDER BY id LIMIT 10"):
                print("  deleted row:", r)
        elif cmd == "edit":
            (rid, desc, lu), = q(cur, "SELECT id, description, last_updated FROM cpd_incidents "
                                      "WHERE NOT deleted AND id <> %s ORDER BY last_updated DESC, id LIMIT 1", (FAKE_ID,))
            cur.execute("UPDATE cpd_incidents SET description = 'SIMULATED STALE VALUE', "
                        "last_updated = last_updated - interval '1 day' WHERE id = %s", (rid,))
            print(f"Made id={rid} stale. Real description starts: {desc[:60]!r}, real last_updated={lu}")
        elif cmd == "remove":
            cur.execute("INSERT INTO cpd_incidents (id, date_time, type, location, description, last_updated, "
                        "year, month, day, hour, minute, second) VALUES (%s, now(), 'INCIDENT', 'FAKE ST', "
                        "'simulated record not in source', now(), 2026, 1, 1, 0, 0, 0) ON CONFLICT DO NOTHING", (FAKE_ID,))
            print(f"Inserted fake active record id={FAKE_ID} (not present in the Cambridge source).")
        elif cmd == "show":
            rid = int(sys.argv[2])
            cols = [d[0] for d in (cur.execute("SELECT * FROM cpd_incidents WHERE id=%s", (rid,)), cur.description)[1]]
            for row in cur.fetchall():
                for c, v in zip(cols, row):
                    print(f"  {c}: {str(v)[:80]}")
    conn.close()


if __name__ == "__main__":
    main()
