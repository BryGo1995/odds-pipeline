#!/usr/bin/env bash
# Back up the production data Postgres (odds_db) with pg_dump.
#
# Each dump is verified with pg_restore --list before it counts, and only the
# newest $BACKUP_KEEP are kept. Dumps live outside the repo so deleting or
# re-cloning the repo can't take them with it.
#
# Optional free offsite copy: set RCLONE_REMOTE to a configured rclone remote,
# e.g. RCLONE_REMOTE=gdrive:odds-backups (Google Drive's free 15 GB is plenty).
#
# Nightly via cron (crontab -e):
#   0 2 * * * /path/to/odds-pipeline/scripts/backup_db.sh >> $HOME/odds-backups/backup.log 2>&1
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-$HOME/odds-backups}"
BACKUP_KEEP="${BACKUP_KEEP:-14}"
CONTAINER="${DATA_DB_CONTAINER:-odds-pipeline-data-postgres-1}"

mkdir -p "$BACKUP_DIR"
dump="$BACKUP_DIR/odds_db_$(date +%Y-%m-%dT%H%M%S).dump"

# Credentials come from the container's own environment, so none live here
docker exec "$CONTAINER" sh -c 'pg_dump -U "$POSTGRES_USER" -Fc "$POSTGRES_DB"' > "$dump.partial"
docker exec -i "$CONTAINER" pg_restore --list < "$dump.partial" > /dev/null
mv "$dump.partial" "$dump"
echo "$(date -Is) wrote $dump ($(du -h "$dump" | cut -f1))"

ls -1t "$BACKUP_DIR"/odds_db_*.dump | tail -n +"$((BACKUP_KEEP + 1))" | xargs -r rm --

if [[ -n "${RCLONE_REMOTE:-}" ]]; then
    rclone copy "$dump" "$RCLONE_REMOTE"
    rclone delete "$RCLONE_REMOTE" --min-age "${BACKUP_KEEP}d" --include 'odds_db_*.dump'
    echo "$(date -Is) copied to $RCLONE_REMOTE"
fi
