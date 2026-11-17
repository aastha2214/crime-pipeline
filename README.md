# Cambridge Police Daily Log: ETL pipeline + dashboard

Extracts the [Cambridge Police Daily Log](https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb)
from the Socrata API, validates and transforms it with pandas, loads it into PostgreSQL, orchestrates
it with Prefect, and visualises it in Metabase. Postgres, pgAdmin and Metabase run in Docker Compose.

```
Socrata API --extract--> validate --> transform --> load (1 transaction) --> Postgres --> Metabase
                          (Prefect flow: retries, backoff, logging, daily schedule)
```

## Deploying to a server

See **DEPLOY_CLOUD.md**: one Ubuntu VM, `./deploy.sh`, everything (including the daily schedule) runs in Docker, and nothing is exposed to the internet.

## Run on another PC

This section is for a beginner. It works on Windows 10/11 and on a Mac. You do not need Python: everything runs inside Docker.

**What you need:** an internet connection, about 6 GB of free disk space, and 8 GB of memory (4 GB can work but is slow). The first run takes roughly 10 to 20 minutes, mostly downloading and building (see "How long it takes" below).

### Step 1. Get the code

Choose ONE way.

- **Download the ZIP:** open the repository page on github.com (sign in, it is private), click the green **Code** button, then **Download ZIP**. Unzip it. Windows: right-click the ZIP, **Extract All**. Mac: double-click the ZIP. You get a folder called `crime-pipeline-main`.
- **Or use git:** install git (Windows: https://git-scm.com/download/win, Mac: type `git --version` in Terminal and accept the offer to install). Then run this, replacing `YOUR-USERNAME`:
  ```
  git clone https://github.com/YOUR-USERNAME/crime-pipeline.git
  ```
  Because the repository is private, git asks you to sign in to GitHub (a browser window opens, or use a personal access token as the password).

### Step 2. Install Docker Desktop

1. Download Docker Desktop from https://www.docker.com/products/docker-desktop/ and install it. Windows: keep the "Use WSL 2" option ticked and restart when asked. Mac: choose the version for your chip (Apple silicon or Intel).
2. Open Docker Desktop and wait until it says it is running (the whale icon stops moving).
3. Check it works. Open a terminal (Windows: search for **PowerShell**; Mac: open **Terminal**) and type:
   ```
   docker --version
   docker compose version
   ```
   Both must print a version number.

### Step 3. Open a terminal inside the project folder

- **Windows (PowerShell):** `cd` to the folder, for example `cd $HOME\Downloads\crime-pipeline-main` (use `crime-pipeline` if you used git clone).
- **Mac (Terminal):** for example `cd ~/Downloads/crime-pipeline-main`.

Check you are in the right place: `dir` (Windows) or `ls` (Mac) must show `docker-compose.yml`.

### Step 4. Create the .env file and fill it in

The `.env` file holds your settings and passwords. It is never uploaded to GitHub, so you must create it on each PC.

- **Windows:** `copy .env.example .env` then `notepad .env`
- **Mac:** `cp .env.example .env` then `open -e .env`

Change these lines, save the file, and close it:

| Setting | What it is | What to put |
|---|---|---|
| `SOCRATA_APP_TOKEN` | A free key for the city's data service (Socrata). **Optional**: the pipeline works without it, but the service may slow it down, and it prints a warning. Leave it empty if you do not have one. | Empty, or a token from https://data.cambridgema.gov/profile/edit/developer_settings |
| `POSTGRES_USER`, `POSTGRES_DB` | User and name of the database that stores the crime data | Keep `cpd_user` and `cpd_db` |
| `POSTGRES_PASSWORD` | Password of that database | Make up a long password (no spaces) |
| `POSTGRES_HOST`, `POSTGRES_PORT` | Where the database is | Keep `localhost` and `5433` (Docker overrides them for you) |
| `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` | Login of pgAdmin, an optional database viewer you do not need | Any email-like text and a password |
| `METABASE_DB_USER`, `METABASE_DB_PASSWORD` | User and password of the small database where Metabase keeps its own settings | Keep `metabase`, make up a password |
| `METABASE_URL` | Address of the dashboard program | Keep `http://localhost:3000` |
| `METABASE_ADMIN_EMAIL` | The email you will type to log in to the dashboard | Your email (or any text that looks like an email) |
| `METABASE_ADMIN_PASSWORD` | The password you will type to log in to the dashboard | A long password with letters and numbers, not an easy word |
| `MAX_DELETE_FRACTION` | A safety limit of the pipeline | Keep `0.5` |

Do not put spaces around the `=` sign, and do not use quotation marks. Write the passwords down: you need the Metabase one to log in.

### Step 5. Start the services

Type these three commands one at a time, in the project folder. The first one is slow the first time (it builds the program and downloads the databases):

```
docker compose build pipeline prefect_server
docker compose up -d --wait --wait-timeout 900 postgres_cpd postgres_mb prefect_server metabase
docker compose ps
```

The last command should show the services as `running` or `healthy`.

### Step 6. Run one data load

This downloads the police log from the city's website, checks and cleans it, and stores it in the database. The first time it loads everything (about 13,000 rows).

```
docker compose run --rm -T pipeline python etl_pipeline.py run
```

It prints log lines and ends without an error message. To see the number of rows, Mac:

```
docker compose exec -T postgres_cpd sh -c 'psql -U "$POSTGRES_USER" -d cpd_db -c "SELECT count(*) FROM cpd_incidents"'
```

Windows PowerShell (replace `cpd_user` if you changed `POSTGRES_USER`):

```
docker compose exec -T postgres_cpd psql -U cpd_user -d cpd_db -c "SELECT count(*) FROM cpd_incidents"
```

### Step 7. Build the dashboard

```
docker compose run --rm -T pipeline python scripts/setup_metabase.py
```

This creates your dashboard login from `METABASE_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD`, connects it to the database, and builds the "Cambridge Police Incidents Dashboard". It prints the web address at the end.

### Step 8. Open the dashboard

Open http://localhost:3000 in your browser. Log in with the email and password you put in `.env`, then open the "Cambridge Police Incidents Dashboard" (use the search box if you do not see it).

To keep the data fresh every day, also run `docker compose up -d pipeline` (it loads new data every midnight US Eastern time while Docker is running).

### How long it takes

Roughly 10 to 20 minutes the first time on a normal home connection: about 5 to 10 minutes to download the images and build, 1 to 3 minutes for the data load, and about 1 minute for the dashboard. Later runs take seconds to a few minutes. Metabase's first page load after starting can take 30 to 60 seconds.

### Stop, restart, and start over

- Stop everything but keep the data: `docker compose stop`
- Start again later: `docker compose up -d --wait postgres_cpd postgres_mb prefect_server metabase`
- Delete everything including the data and start over (careful, this erases the database): `docker compose down -v`

### If something fails

- **"Cannot connect to the Docker daemon" or "docker daemon is not running":** Docker Desktop is not running. Open Docker Desktop, wait until it says it is running, and repeat the command.
- **"port is already allocated" or "address already in use":** another program uses port 3000 (Metabase), 5433 (database), 4200 (Prefect) or 8081. Stop the other program (or an older copy of this project: `docker compose down`) and run the command again.
- **The dashboard page does not open or shows a loading message:** wait one minute and refresh. Check `docker compose ps`: `metabase` must say `healthy`.
- **A load is slow or says "rate limit" or "429":** the city's server is throttling you. Add a free `SOCRATA_APP_TOKEN` to `.env`, or wait a few minutes and run the load again. It is safe to repeat.
- **"change_me" or empty password error, or the login does not work:** check `.env` for typos and for spaces around `=`. If you changed a password after the first start, run `docker compose down -v` and start again from Step 5.
- **Windows: "WSL 2 installation is incomplete" or "virtualization":** open PowerShell as administrator, run `wsl --install`, restart the PC, and open Docker Desktop again. Virtualization must be enabled in the BIOS.
- **See what went wrong:** `docker compose logs --tail 100 pipeline` (or `metabase`).

## Files

| File | Purpose |
|---|---|
| `config.py` | Settings, expected schema, logger. Import it first (it sets `PREFECT_HOME` and loads `.env`). |
| `extract.py` | Socrata calls. Classifies errors as retryable (timeout, 429, 5xx) or permanent (bad token, 404). Fetches changed rows, all source ids, by-id backfills, and the schema from dataset metadata. |
| `validate.py` | Schema-drift detection (reports added/removed/renamed columns), required values, ISO 8601 dates, numeric ids. |
| `transform.py` | Drops bad ids, dedupes by id (latest `last_updated` wins), derives year/month/day/hour/minute/second. |
| `load.py` | Creates the table, upserts on `id`, flags vanished records as deleted, all in one transaction. |
| `etl_pipeline.py` | The Prefect flow and daily schedule. |
| `docker-compose.yml` | `postgres_cpd`, `postgres_mb`, `metabase`, `prefect_server` (the scheduler), `pipeline` (the daily flow), and optional `pgadmin` (`--profile admin`). Healthchecks, restart policies, localhost-only ports. |
| `Dockerfile`, `deploy.sh`, `DEPLOY_CLOUD.md` | Cloud deployment (see above). |
| `scripts/setup_metabase.py` | Creates the Metabase admin user, connects the DB, builds the dashboard via REST API (idempotent). |
| `scripts/simulate_changes.py` | Simulates an edited record / a removed record against the DB, plus a `report`. |
| `scripts/psql.sh` | `psql` shell inside the Postgres container. |
| `tests/` | Offline unit tests (no network or DB needed). |

## How it handles change

- **New and edited records:** `INSERT ... ON CONFLICT (id) DO UPDATE`, applied only when the source `last_updated` differs. Running twice cannot create duplicates because `id` is the primary key.
- **Incremental runs:** the watermark is `max(last_updated)` over active rows in the DB. The first run (empty table) is a full load. Later runs fetch `last_updated >= watermark`. It uses `>=` because many rows share one timestamp, and re-fetched rows are harmless thanks to the upsert.
- **Disappeared records:** every run fetches the full list of source ids. Active DB rows whose id is gone get `deleted = TRUE, deleted_at = now()`. They are never hard-deleted, and every dashboard query filters `WHERE NOT deleted`. If a record comes back, it is un-flagged automatically.
- **Self-healing:** ids that are in the source but missing from the DB, or flagged deleted, are fetched by id even when their `last_updated` is older than the watermark.
- **Schema changes:** the source's real columns (dataset metadata) are compared to `EXPECTED_COLUMNS`. A mismatch fails the run before loading, and the log lists removed columns, added columns and likely renames. (Socrata omits all-null columns from row JSON, which is why metadata is used rather than inferring from data.)
- **Reliability:** API tasks retry 4 times with exponential backoff (about 10/20/40/80s plus jitter), only for transient errors. The DB task retries on connection errors. All DB writes for a run are in a single transaction (rolled back on any error). A delete-safety guard refuses to flag more than `MAX_DELETE_FRACTION` (default 50%) of active rows, or everything if the source returns zero ids, since that looks like a truncated API response and not a real purge. A flow `on_failure` hook logs a one-line failure reason.

## Setup from scratch

```bash
# 1. Python env (3.12)
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. Config: copy and fill in. .env is git-ignored.
cp .env.example .env
#    - set SOCRATA_APP_TOKEN (get one at https://data.cambridgema.gov/profile/edit/developer_settings)
#    - choose passwords

# 3. Start Postgres, pgAdmin, Metabase
docker compose up -d

# 4. Load the data (first run = full load)
.venv/bin/python etl_pipeline.py run

# 5. Build the dashboard (creates the Metabase admin from .env on first run)
.venv/bin/python scripts/setup_metabase.py        # prints the dashboard URL
```

## Day-to-day

```bash
.venv/bin/python etl_pipeline.py run                 # one incremental run now
.venv/bin/python etl_pipeline.py run --full-refresh  # ignore watermark, refetch everything
# daily schedule (midnight, America/New_York): needs a Prefect server running, otherwise nothing is ever triggered.
# In one terminal:  PREFECT_HOME=$PWD/.prefect .venv/bin/prefect server start
# In another (with PREFECT_API_URL=http://127.0.0.1:4200/api set):  .venv/bin/python etl_pipeline.py
# (On a server, use the Docker setup instead: see DEPLOY_CLOUD.md.)
.venv/bin/python -m pytest tests                     # offline tests
scripts/psql.sh -c "SELECT count(*) FROM cpd_incidents WHERE NOT deleted"
docker compose down                                  # stop (data persists in volumes); add -v to wipe data
```

`run` works without a server (it uses a temporary in-process one). The *schedule* does not: `python etl_pipeline.py` on its own only serves the flow and never triggers it, because Prefect cannot schedule on its temporary server. Run a real Prefect server (above), or use the Docker deployment, which includes one.

Services: Metabase http://localhost:3000, Prefect API http://localhost:4200/docs (a local `prefect server start` also serves the web UI at `/`; the AWS deployment did not, cause unknown), Postgres localhost:5433, and pgAdmin http://localhost:8081 only if started with `docker compose --profile admin up -d pgadmin` (all bound to 127.0.0.1).
Credentials are in `.env`. In pgAdmin, register the server with host `postgres_cpd`, port `5432`.

## Simulating changes (what I used to verify)

```bash
.venv/bin/python scripts/simulate_changes.py edit      # make the newest record stale in the DB
.venv/bin/python etl_pipeline.py run                   # -> "updated: 1"
.venv/bin/python scripts/simulate_changes.py remove    # add a fake record absent from the source
.venv/bin/python etl_pipeline.py run                   # -> "deleted: 1", row flagged, not removed
.venv/bin/python scripts/simulate_changes.py report
```

## Dashboard SQL (fallback if the API route ever fails)

In Metabase: **New → SQL query**, pick the "Cambridge PD" database, paste, **Visualize**, **Save**, then add to a new dashboard.
(If the database isn't connected: Admin → Databases → Add → PostgreSQL, host `postgres_cpd`, port `5432`, db `cpd_db`, credentials from `.env`.)

```sql
-- Incidents by hour of day (bar)
SELECT hour AS hour_of_day, count(*) AS incidents FROM cpd_incidents WHERE NOT deleted GROUP BY hour ORDER BY hour;

-- Incidents by type (row/bar)
SELECT type, count(*) AS incidents FROM cpd_incidents WHERE NOT deleted GROUP BY type ORDER BY incidents DESC LIMIT 15;

-- Incidents by month (line)
SELECT date_trunc('month', date_time)::date AS month, count(*) AS incidents FROM cpd_incidents WHERE NOT deleted GROUP BY 1 ORDER BY 1;

-- Top locations (row/bar)
SELECT location, count(*) AS incidents FROM cpd_incidents WHERE NOT deleted GROUP BY location ORDER BY incidents DESC LIMIT 15;
```

## Bugs fixed relative to the notebook

- `load.py`: nested double quotes inside an f-string (a syntax error before Python 3.12) → replaced by psycopg2 connection params.
- `to_sql(if_exists='replace')` dropped the primary key and the whole table each run → explicit `CREATE TABLE` + `ON CONFLICT` upsert.
- Compose healthcheck `interval: 15` → `15s`; `metabase` had no `depends_on` → now waits for healthy Postgres services (which got healthchecks).
- `etl_pipeline.py` imports were commented out → active.
- Validation order: ids were coerced to NaN and *then* checked for missing values, so a non-numeric id crashed the run despite the documented "drop it later" intent → missing-value check now runs on raw values first.
- `check_valid_schema` inferred schema from rows, which false-alarms when Socrata omits all-null columns → uses dataset metadata.
- Dedupe kept the *first* duplicate; now keeps the most recently updated.
- Postgres/pgAdmin/Metabase ports now bound to 127.0.0.1 only; unneeded `/dev/urandom` mount and the unused metabase-db host port removed; Metabase image pinned.
