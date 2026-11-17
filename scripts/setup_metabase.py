"""Create the Metabase admin user (first run), connect the crime database, and build the dashboard via REST API.

Safe to rerun: existing database connection / cards / dashboard are updated in place, not duplicated.
Reads credentials from .env (METABASE_ADMIN_*, POSTGRES_*). Run after `docker compose up -d` and the first ETL run.
"""
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BASE = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
EMAIL = os.getenv("METABASE_ADMIN_EMAIL")
PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD")
DB_NAME = "Cambridge PD"
DASHBOARD_NAME = "Cambridge Police Incidents Dashboard"

# --- Cards -------------------------------------------------------------------------------------------------
# Every query excludes records flagged deleted (NOT deleted).
# Every query also carries three OPTIONAL field-filter clauses. `[[AND {{x}}]]` disappears when the dashboard filter
# is empty, so they are no-ops until someone picks a value. This is what lets dashboard filters drive SQL charts.
FILTERS = "[[AND {{date_range}}]] [[AND {{incident_type}}]] [[AND {{location}}]]"
WHERE = f"WHERE NOT deleted {FILTERS}"
TAGS = {  # template-tag name -> (column in cpd_incidents, widget type)
    "date_range": ("date_time", "date/all-options"),
    "incident_type": ("type", "string/="),
    "location": ("location", "string/="),  # dropdown, multi-select
}

# Notes. Metabase has no native chart subtitle, so each is shown as a thin text tile directly above its chart
# (see build_layout) and also saved as the card description (the "i" tooltip). Text tiles are styled with the only
# options Metabase text cards support: markdown (bold/italic), no card background (so they don't look like white
# chart tiles) and text alignment. No colour, font-size or border setting exists.
NOTE_H = 1  # grid rows per note tile


def note_md(text):
    return f"**Note:** *{text}*"


def note_tile_settings(text):
    style = {"dashcard.background": False, "text.align_vertical": "middle", "text.align_horizontal": "left"}
    return {"text": note_md(text), **style,
            "virtual_card": {"name": None, "display": "text", "dataset_query": {}, "archived": False,
                             "visualization_settings": style}}
SUBTITLES = {
    "Incidents by hour of day": "Incidents recorded in each hour of the day (00:00 to 23:00).",
    "Average incidents per day, by month":
        "Average incidents per day. The latest month counts only days with published data.",
    "Incidents by type (log scale)":
        "Log scale: INCIDENT is far larger than the other types, so bar heights are not proportional.",
    "Top 15 locations":
        "Counted by exact location text. Intersections and masked blocks (e.g. 5XX) are separate entries.",
    "Top 10 incident subtypes": "Top 10 most frequent subtypes. Rows without a subtype are excluded.",
}

CARDS = [
    # --- KPI cards, calculated live by Metabase on every load ---
    dict(name="Total incidents", display="scalar", sql=f"SELECT count(*) AS total_incidents FROM cpd_incidents {WHERE}"),
    dict(name="Unique locations", display="scalar",
         sql=f"SELECT count(DISTINCT location) AS unique_locations FROM cpd_incidents {WHERE}"),
    dict(name="Peak incident hour", display="scalar",  # shown like 15:00; ties go to the earlier hour
         sql=f"SELECT lpad(hour::text, 2, '0') || ':00' AS peak_incident_hour FROM cpd_incidents {WHERE} "
             "GROUP BY hour ORDER BY count(*) DESC, hour LIMIT 1"),
    # Subtype is far more informative than type (INCIDENT is ~95% of rows). NULL subtypes are not a value.
    # Big-number (scalar) card like the other three KPIs: the most frequent value of the type column (currently
    # INCIDENT). NULL/blank excluded; ties go to the alphabetically first type. Renamed in place from earlier
    # versions of this card (subtype variants), so no duplicate card is created.
    dict(name="Most common incident type",
         former_names=["Top subtype count", "Most common incident subtype"], display="scalar",
         sql=f"SELECT type AS most_common_incident_type FROM cpd_incidents {WHERE} "
             "AND type IS NOT NULL AND btrim(type) <> '' "
             "GROUP BY type ORDER BY count(*) DESC, type LIMIT 1"),

    # --- Charts ---
    # All 24 hours, in order, labelled 00:00..23:00. generate_series + LEFT JOIN keeps hours that have zero incidents.
    dict(name="Incidents by hour of day", display="bar", dims=["hour_of_day"], metrics=["incidents"],
         settings={"graph.x_axis.title_text": "Hour of day", "graph.y_axis.title_text": "Incidents",
                   "graph.x_axis.axis_enabled": "rotate-45",  # rotate so all 24 labels fit instead of being skipped
                   "graph.label_value_formatting": "full"},
         sql="SELECT lpad(h::text, 2, '0') || ':00' AS hour_of_day, count(c.hour) AS incidents "
             f"FROM generate_series(0, 23) AS h LEFT JOIN (SELECT hour FROM cpd_incidents {WHERE}) c ON c.hour = h "
             "GROUP BY h ORDER BY h"),
    # Average incidents per day. The source publishes with a lag, so the latest month is partial. Each month divides
    # by the days from the 1st to min(end of month, latest date with data in the WHOLE table): calendar days for
    # full months, days-with-data for the latest month. `mx` is deliberately unfiltered.
    dict(name="Average incidents per day, by month", former_names=["Incidents by month"],
         display="line", dims=["month"], metrics=["avg_incidents_per_day"],
         settings={"graph.x_axis.title_text": "Month", "graph.y_axis.title_text": "Avg incidents per day"},
         sql="WITH m AS (SELECT date_trunc('month', date_time)::date AS month, count(*) AS n "
             f"FROM cpd_incidents {WHERE} GROUP BY 1), "
             "mx AS (SELECT max(date_time)::date AS d FROM cpd_incidents WHERE NOT deleted) "
             "SELECT m.month, round(m.n::numeric / LEAST((m.month + interval '1 month')::date - m.month, "
             "mx.d - m.month + 1), 1) AS avg_incidents_per_day FROM m, mx ORDER BY m.month"),
    # Counts range from ~12k to 2, so use a log y-axis to keep the small types visible.
    # (Metabase supports log scale on bar charts but not horizontal row charts.)
    dict(name="Incidents by type (log scale)", former_names=["Incidents by type"],
         display="bar", dims=["type"], metrics=["incidents"],
         settings={"graph.y_axis.scale": "log", "graph.x_axis.title_text": "Type",
                   "graph.y_axis.title_text": "Incidents (log scale)"},
         sql=f"SELECT type, count(*) AS incidents FROM cpd_incidents {WHERE} "
             "GROUP BY type ORDER BY incidents DESC, type LIMIT 15"),
    # Row charts: first row is drawn at the top, so ORDER BY count DESC puts the biggest bar first.
    dict(name="Top 15 locations", display="row", dims=["location"], metrics=["incidents"],
         settings={"graph.x_axis.title_text": "Location", "graph.y_axis.title_text": "Incidents",
                   "graph.show_values": True, "graph.label_value_formatting": "full"},
         sql=f"SELECT location, count(*) AS incidents FROM cpd_incidents {WHERE} "
             "GROUP BY location ORDER BY incidents DESC, location LIMIT 15"),
    # NULL subtypes are not a "value", so excluded. Linear scale (default).
    dict(name="Top 10 incident subtypes", display="row", dims=["subtype"], metrics=["incidents"],
         settings={"graph.x_axis.title_text": "Subtype", "graph.y_axis.title_text": "Incidents",
                   "graph.show_values": True, "graph.label_value_formatting": "full"},
         sql=f"SELECT subtype, count(*) AS incidents FROM cpd_incidents {WHERE} AND subtype IS NOT NULL "
             "GROUP BY subtype ORDER BY incidents DESC, subtype LIMIT 10"),
]

# --- Dashboard filters (connected to every card above, since every query has the three field-filter clauses) ---
# dashboard filter id -> SQL template-tag it drives. The Location filter id is deliberately NOT "location":
# Metabase remembers each user's last-used filter values per filter id, and an old id kept stale values alive.
TAG_FOR = {"date_range": "date_range", "incident_type": "incident_type", "location_select": "location"}

def build_parameters(locations):
    """No filter has a "default" key, so the dashboard opens unfiltered."""
    return [
        {"id": "date_range", "name": "Date range", "slug": "date_range", "type": "date/all-options",
         "sectionId": "date"},
        {"id": "incident_type", "name": "Incident type", "slug": "incident_type", "type": "string/=",
         "sectionId": "string", "values_query_type": "list"},
        # Multi-select dropdown fed by a COMPLETE static list of real locations (read from the DB on every run),
        # instead of the field's own value cache. That cache, and a saved-question source, both stop at 1,000 of
        # ~1,370 values; when the list is truncated Metabase turns the box into a type-ahead that accepts typed text
        # as a value. A complete list is shown as pick-from-list only. Trade-off: the list is a snapshot, so rerun
        # this script after loads that add new locations.
        {"id": "location_select", "name": "Location", "slug": "location", "type": "string/=", "sectionId": "string",
         "values_query_type": "list", "isMultiSelect": True, "values_source_type": "static-list",
         "values_source_config": {"values": locations}},
    ]


# --- Layout on Metabase's 24-column grid: (kind, name-or-text, row, col, width, height) ---
# "header" and "subtitle" are text tiles; "card" is a saved question wired to all three filters.
# Thousands separators on every number (12,493). Column name -> decimals. Applied to any card whose SQL outputs it,
# and the data labels on charts use full numbers instead of 12k-style abbreviations.
NUMBER_COLUMNS = {"total_incidents": 0, "unique_locations": 0, "incidents": 0, "avg_incidents_per_day": 1}
for _spec in CARDS:
    cs = {f'["name","{col}"]': {"number_style": "decimal", "number_separators": ".,", "decimals": dec}
          for col, dec in NUMBER_COLUMNS.items() if f" AS {col}" in _spec["sql"]}
    if cs:
        _spec.setdefault("settings", {})["column_settings"] = cs
    if _spec["display"] in ("bar", "line", "row"):
        _spec.setdefault("settings", {}).setdefault("graph.label_value_formatting", "full")


def build_layout(source_note):
    """Tiles as (kind, name-or-text, row, col, width, height) on the 24-column grid. Each note sits directly above
    its chart with the chart's width; every row starts where the previous one ends, so gutters are identical."""
    sub, h, out, y = SUBTITLES, NOTE_H, [], 0

    def band(tiles, height):
        nonlocal y
        out.extend((kind, what, y, col, w, height) for kind, what, col, w in tiles)
        y += height

    band([("text", source_note, 0, 24)], h)
    band([("card", "Total incidents", 0, 6), ("card", "Unique locations", 6, 6),
          ("card", "Peak incident hour", 12, 6), ("card", "Most common incident type", 18, 6)], 4)
    band([("text", sub["Incidents by hour of day"], 0, 12),
          ("text", sub["Average incidents per day, by month"], 12, 12)], h)
    band([("card", "Incidents by hour of day", 0, 12), ("card", "Average incidents per day, by month", 12, 12)], 8)
    band([("text", sub["Incidents by type (log scale)"], 0, 12), ("text", sub["Top 15 locations"], 12, 12)], h)
    band([("card", "Incidents by type (log scale)", 0, 12), ("card", "Top 15 locations", 12, 12)], 8)
    band([("text", sub["Top 10 incident subtypes"], 0, 24)], h)
    band([("card", "Top 10 incident subtypes", 0, 24)], 12)  # taller so the long subtype labels stay readable
    return out


def fail(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def call(s, method, path, **kw):
    r = s.request(method, f"{BASE}{path}", timeout=60, **kw)
    if not r.ok:
        fail(f"{method} {path} -> HTTP {r.status_code}: {r.text[:400]}")
    return r.json() if r.text else None


def wait_for_metabase():
    print("Waiting for Metabase to be healthy (first boot can take a few minutes)...")
    for _ in range(60):
        try:
            if requests.get(f"{BASE}/api/health", timeout=5).json().get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(5)
    fail("Metabase did not become healthy in 5 minutes. Check `docker compose logs metabase`.")


def login():
    s = requests.Session()
    token = requests.get(f"{BASE}/api/session/properties", timeout=30).json().get("setup-token")
    if token:
        print("Running one-time Metabase setup (creating admin user)...")
        call(s, "POST", "/api/setup", json={
            "token": token,
            "user": {"first_name": "Admin", "last_name": "User", "email": EMAIL, "password": PASSWORD,
                     "site_name": "Cambridge Crime Trends"},
            "prefs": {"site_name": "Cambridge Crime Trends", "site_locale": "en", "allow_tracking": False},
        })
    r = s.post(f"{BASE}/api/session", json={"username": EMAIL, "password": PASSWORD}, timeout=30)
    if not r.ok:
        fail("Could not log in to Metabase with METABASE_ADMIN_EMAIL / METABASE_ADMIN_PASSWORD from .env. "
             "If setup was already completed with different credentials, update .env or log in manually.")
    s.headers["X-Metabase-Session"] = r.json()["id"]
    return s


def ensure_database(s):
    dbs = call(s, "GET", "/api/database")
    dbs = dbs["data"] if isinstance(dbs, dict) else dbs
    for d in dbs:
        if d["name"] == DB_NAME:
            return d["id"]
    print("Connecting Metabase to the crime database...")
    d = call(s, "POST", "/api/database", json={
        "engine": "postgres", "name": DB_NAME, "is_full_sync": True,
        # inside the docker network the Postgres container is reachable as postgres_cpd:5432
        "details": {"host": "postgres_cpd", "port": 5432, "dbname": os.getenv("POSTGRES_DB", "cpd_db"),
                    "user": os.getenv("POSTGRES_USER"), "password": os.getenv("POSTGRES_PASSWORD"), "ssl": False},
    })
    call(s, "POST", f"/api/database/{d['id']}/sync_schema")
    return d["id"]


def upsert_card(s, db_id, field_ids, spec, existing):
    tags = {} if spec.get("no_filters") else {
        tag: {"id": f"tag-{tag}", "name": tag, "display-name": tag.replace("_", " ").title(), "type": "dimension",
              "dimension": ["field", field_ids[col], None], "widget-type": widget,
              # "contains" is case-sensitive unless told otherwise; "mass" must match MASSACHUSETTS AVE
              **({"options": {"case-sensitive": False}} if widget == "string/contains" else {})}
        for tag, (col, widget) in TAGS.items()
    }
    settings = dict(spec.get("settings", {}))
    if "dims" in spec:
        settings.update({"graph.dimensions": spec["dims"], "graph.metrics": spec["metrics"]})
    body = {
        "name": spec["name"], "display": spec["display"], "collection_id": None,
        "description": SUBTITLES.get(spec["name"]),
        "dataset_query": {"type": "native", "database": db_id,
                          "native": {"query": spec["sql"], "template-tags": tags}},
        "visualization_settings": settings,
    }
    # match by current name, else by a former name so renames update the card in place (no duplicates)
    match = next((n for n in [spec["name"], *spec.get("former_names", [])] if n in existing), None)
    if match:
        return call(s, "PUT", f"/api/card/{existing[match]}", json=body)["id"]
    return call(s, "POST", "/api/card", json=body)["id"]


def field_ids_for(s, db_id, timeout=300):
    """Field ids of cpd_incidents. On a fresh Metabase the new database is scanned in the background, so the table
    is not listed for a while: wait for it (re-triggering the scan once) instead of failing."""
    need = {name for name, _ in TAGS.values()}
    deadline, retriggered = time.time() + timeout, False
    while True:
        meta = call(s, "GET", f"/api/database/{db_id}/metadata")
        table = next((t for t in meta["tables"] if t["name"] == "cpd_incidents"), None)
        fields = {f["name"]: f["id"] for f in table["fields"]} if table else {}
        if need <= set(fields):
            return fields
        if time.time() > deadline:
            fail("Metabase has not finished scanning the cpd_incidents table after "
                 f"{timeout}s. Check `docker compose logs metabase`, then rerun this script.")
        if not retriggered:
            call(s, "POST", f"/api/database/{db_id}/sync_schema")
            retriggered = True
        print("Waiting for Metabase to finish scanning the database...")
        time.sleep(5)


def distinct_locations(s, db_id):
    """Every distinct location currently in the table (excluding deleted rows), for the dropdown's static list."""
    q = "SELECT DISTINCT location FROM cpd_incidents WHERE NOT deleted ORDER BY location"
    res = call(s, "POST", "/api/dataset", json={"database": db_id, "type": "native", "native": {"query": q},
                                                  "constraints": {"max-results": 100000, "max-results-bare-rows": 100000}})
    rows = res["data"]["rows"]
    if len(rows) != res["row_count"] or res["data"].get("results_truncated"):
        fail("Location list was truncated by Metabase; refusing to build a partial dropdown.")
    return [r[0] for r in rows]


def source_note(s, db_id):
    """Header text with the data's date range, read from the database on every run."""
    q = "SELECT min(date_time)::date, max(date_time)::date FROM cpd_incidents WHERE NOT deleted"
    res = call(s, "POST", "/api/dataset", json={"database": db_id, "type": "native", "native": {"query": q}})
    first, last = (datetime.strptime(v[:10], "%Y-%m-%d") for v in res["data"]["rows"][0])
    fmt = lambda d: f"{d:%b} {d.day}, {d.year}"  # noqa: E731
    return (f"Source: Cambridge Police Daily Log. Data covers {fmt(first)} to {fmt(last)}, filled in from the "
            "database on each run. The source publishes with a delay, so the latest days may be missing.")


def main():
    if not EMAIL or not PASSWORD:
        fail("Set METABASE_ADMIN_EMAIL and METABASE_ADMIN_PASSWORD in .env first.")
    wait_for_metabase()
    s = login()
    db_id = ensure_database(s)

    field_ids = field_ids_for(s, db_id)
    existing = {c["name"]: c["id"] for c in call(s, "GET", "/api/card") if not c.get("archived")}
    card_ids = {spec["name"]: upsert_card(s, db_id, field_ids, spec, existing) for spec in CARDS}
    PARAMETERS = build_parameters(distinct_locations(s, db_id))
    print("Cards:", card_ids)

    dash = next((d for d in call(s, "GET", "/api/dashboard") if d["name"] == DASHBOARD_NAME), None)
    dash_id = dash["id"] if dash else call(s, "POST", "/api/dashboard", json={"name": DASHBOARD_NAME})["id"]

    dashcards = []
    for i, (kind, what, row, col, w, h) in enumerate(build_layout(source_note(s, db_id))):
        tile = {"id": -(i + 1), "row": row, "col": col, "size_x": w, "size_y": h, "series": []}
        if kind == "text":
            tile.update(card_id=None, parameter_mappings=[], visualization_settings=note_tile_settings(what))
        else:
            cid = card_ids[what]
            tile.update(card_id=cid, visualization_settings={}, parameter_mappings=[
                {"parameter_id": p["id"], "card_id": cid, "target": ["dimension", ["template-tag", TAG_FOR[p["id"]]]]}
                for p in PARAMETERS])
        dashcards.append(tile)
    call(s, "PUT", f"/api/dashboard/{dash_id}", json={"parameters": PARAMETERS, "dashcards": dashcards})
    # METABASE_PUBLIC_URL = the address a person types in their browser (inside Docker, BASE is the internal one)
    print(f"\nDashboard ready: {os.getenv('METABASE_PUBLIC_URL', BASE).rstrip('/')}/dashboard/{dash_id}")


if __name__ == "__main__":
    main()
