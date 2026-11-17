#!/bin/sh
# Run psql inside the postgres container (local socket auth; no password on the command line).
. "$(dirname "$0")/../.env"
exec docker exec -i postgres_cpd_dev psql -U "$POSTGRES_USER" -d cpd_db "$@"
