#!/usr/bin/env bash
# One-command deploy for an Ubuntu VM (works on any machine with Docker + the Compose plugin).
# Safe to rerun: every step is idempotent (the data load is an upsert, the Metabase setup updates in place).
#
#   ./deploy.sh
#
# What it does: checks Docker -> prepares .env -> builds the pipeline image -> starts Postgres, Metabase and the
# Prefect scheduler -> runs the first data load -> starts the daily-schedule service -> builds the dashboard.
set -euo pipefail
cd "$(dirname "$0")"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
die() { printf '\n\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }
env_val() { grep -E "^$1=" .env | head -1 | cut -d= -f2- || true; }
rand() { if command -v openssl >/dev/null 2>&1; then openssl rand -hex 12; else head -c 64 /dev/urandom | od -An -tx1 | tr -d ' \n' | head -c 24; fi; }

# ---- 1. Docker ---------------------------------------------------------------------------------------------------
say "Checking Docker"
if ! command -v docker >/dev/null 2>&1; then
  die "Docker is not installed. On Ubuntu run:  curl -fsSL https://get.docker.com | sudo sh   then  sudo usermod -aG docker \$USER
       and log out and back in. (Full steps: DEPLOY_CLOUD.md, step 3.)"
fi
docker compose version >/dev/null 2>&1 || die "The 'docker compose' plugin is missing. Install it with: sudo apt-get install -y docker-compose-plugin"
if ! docker info >/dev/null 2>&1; then
  die "Cannot talk to the Docker daemon. If you see 'permission denied', run: sudo usermod -aG docker \$USER  then log out and back in.
       If Docker is not running: sudo systemctl enable --now docker"
fi
echo "Docker OK: $(docker --version)"

# ---- 2. .env -----------------------------------------------------------------------------------------------------
say "Checking .env"
if [ ! -f .env ]; then
  [ -f .env.example ] || die ".env.example is missing; cannot create .env."
  cp .env.example .env
  # random passwords for everything local; the Socrata token is left for YOU to fill in
  sed -i.bak \
    -e "s|^SOCRATA_APP_TOKEN=.*|SOCRATA_APP_TOKEN=|" \
    -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$(rand)|" \
    -e "s|^PGADMIN_DEFAULT_PASSWORD=.*|PGADMIN_DEFAULT_PASSWORD=$(rand)|" \
    -e "s|^METABASE_DB_PASSWORD=.*|METABASE_DB_PASSWORD=$(rand)|" \
    -e "s|^METABASE_ADMIN_PASSWORD=.*|METABASE_ADMIN_PASSWORD=Cpd-$(rand)|" .env
  rm -f .env.bak
  chmod 600 .env
  cat <<'MSG'

I created .env with randomly generated passwords. One thing is left for you:

  1. Open it:           nano .env
  2. Set your Socrata app token on the SOCRATA_APP_TOKEN= line (and, if you want, change METABASE_ADMIN_EMAIL).
  3. Save (Ctrl+O, Enter) and exit (Ctrl+X), then run this script again:   ./deploy.sh

MSG
  exit 0
fi
chmod 600 .env
for key in POSTGRES_PASSWORD METABASE_DB_PASSWORD METABASE_ADMIN_PASSWORD PGADMIN_DEFAULT_PASSWORD; do
  v="$(env_val "$key")"
  if [ -z "$v" ] || [ "$v" = "change_me" ] || [ "$v" = "change_me_to_a_long_password" ]; then
    die "$key in .env is empty or still a placeholder. Set a real value (a long random string) and rerun."
  fi
done
token="$(env_val SOCRATA_APP_TOKEN)"
if [ -z "$token" ] || [ "$token" = "your_socrata_app_token_here" ]; then
  echo "WARNING: SOCRATA_APP_TOKEN is empty. The pipeline will still work but Socrata may throttle it."
  if [ -t 0 ]; then
    read -r -p "Continue without a token? [y/N] " ans
    [ "$ans" = "y" ] || [ "$ans" = "Y" ] || die "Stopped. Add the token to .env (nano .env) and rerun."
  fi
fi
echo ".env OK"

# ---- 3. build + start the long-running services -------------------------------------------------------------------
say "Building the pipeline image (first time takes a few minutes)"
docker compose build pipeline prefect_server

say "Starting Postgres, Metabase and the Prefect scheduler (waiting until they are healthy)"
docker compose up -d --wait --wait-timeout 900 postgres_cpd postgres_mb prefect_server metabase

# ---- 4. first data load ------------------------------------------------------------------------------------------
say "Running the first data load (full load on an empty database; incremental afterwards)"
docker compose run --rm -T pipeline python etl_pipeline.py run

# ---- 5. daily schedule ---------------------------------------------------------------------------------------------
say "Starting the daily schedule service"
docker compose up -d pipeline

# ---- 6. dashboard --------------------------------------------------------------------------------------------------
say "Setting up Metabase (admin user, database connection, dashboard)"
docker compose run --rm -T pipeline python scripts/setup_metabase.py

if [ -n "$(env_val SITE_DOMAIN)" ]; then
  say "Starting the public HTTPS proxy (Caddy) for $(env_val SITE_DOMAIN)"
  docker compose --profile public up -d caddy
fi

say "Status"
docker compose ps

cat <<MSG

============================================================================================================
DONE. Next steps
============================================================================================================
Nothing is exposed to the internet: every port is bound to 127.0.0.1 on this server. To open the web pages,
run this on YOUR OWN computer (not on the server), then leave that window open:

    ssh -L 3000:localhost:3000 -L 4200:localhost:4200  YOUR_USER@YOUR_SERVER_IP

  Dashboard (Metabase):  http://localhost:3000     (look for "Cambridge Police Incidents Dashboard")
        login: $(env_val METABASE_ADMIN_EMAIL)   password: the METABASE_ADMIN_PASSWORD line in .env on this server
  Prefect API docs:      http://localhost:4200/docs   (the Prefect web page was NOT available on the AWS server; check runs with the curl command in DEPLOY_CLOUD.md, step 9)

The pipeline runs by itself every day at midnight US Eastern time. Useful commands (run in this folder):

    docker compose logs -f pipeline            # watch the scheduler
    docker compose run --rm -T pipeline python etl_pipeline.py run     # run the load right now
    docker compose exec -T postgres_cpd sh -c 'psql -U "\$POSTGRES_USER" -d cpd_db -c "SELECT count(*), max(date_time) FROM cpd_incidents WHERE NOT deleted"'
    docker compose ps                          # is everything up?

After a daily load adds new locations, rerun ./deploy.sh (or just the setup_metabase.py step) to refresh the
dashboard's Location dropdown list. More details, backups and troubleshooting: DEPLOY_CLOUD.md
MSG
