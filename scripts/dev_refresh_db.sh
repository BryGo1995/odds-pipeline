#!/usr/bin/env bash
# Replace the dev stack's database with the newest production backup.
# Run scripts/backup_db.sh first if you want today's data.
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-$HOME/odds-backups}"
latest="$(ls -1t "$BACKUP_DIR"/odds_db_*.dump 2>/dev/null | head -n 1)"
if [[ -z "$latest" ]]; then
    echo "No backups in $BACKUP_DIR — run scripts/backup_db.sh first" >&2
    exit 1
fi

compose=(docker compose -f "$(dirname "$0")/../docker-compose.dev.yml")
"${compose[@]}" up -d --wait dev-postgres
"${compose[@]}" exec -T dev-postgres sh -c \
    'dropdb -U "$POSTGRES_USER" --if-exists --force "$POSTGRES_DB" && createdb -U "$POSTGRES_USER" "$POSTGRES_DB"'
"${compose[@]}" exec -T dev-postgres sh -c \
    'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner --no-privileges --exit-on-error' < "$latest"
echo "Restored $latest into dev-postgres"
