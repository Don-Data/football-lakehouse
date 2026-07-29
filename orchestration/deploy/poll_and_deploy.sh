#!/bin/bash
set -euo pipefail

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$HOME/.orbstack/bin:$PATH"

REPO_DIR="$HOME/Repos/football-lakehouse"
ORCH_DIR="$REPO_DIR/orchestration"
LOG_FILE="$HOME/Library/Logs/football-lakehouse-deploy.log"
PROJECT_PREFIX="football-lakehouse_4f3f86"

log() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $1" >> "$LOG_FILE"
}

cd "$REPO_DIR"

OLD_HEAD=$(git rev-parse HEAD)
git fetch origin master --quiet
NEW_HEAD=$(git rev-parse origin/master)

if [ "$OLD_HEAD" = "$NEW_HEAD" ]; then
    exit 0
fi

log "New commits found: $OLD_HEAD -> $NEW_HEAD"
git pull origin master --quiet

if git diff --name-only "$OLD_HEAD" "$NEW_HEAD" -- orchestration/requirements.txt orchestration/Dockerfile orchestration/packages.txt | grep -q .; then
    log "Dependency files changed, rebuild+restart needed"

    RUNNING_COUNT=$(docker exec "${PROJECT_PREFIX}-postgres-1" psql -U postgres -d postgres -tAc \
        "SELECT count(*) FROM dag_run WHERE dag_id='football_lakehouse' AND state='running'" 2>/dev/null || echo "1")

    if [ "${RUNNING_COUNT:-1}" -gt 0 ]; then
        log "A DAG run is currently active (count=$RUNNING_COUNT), skipping restart this cycle"
        exit 0
    fi

    log "Restarting Airflow to pick up dependency changes"
    cd "$ORCH_DIR"
    astro dev restart >> "$LOG_FILE" 2>&1
    log "Restart complete"
else
    log "No dependency changes - DAG file changes will be picked up automatically by Airflow's periodic re-scan, no restart needed"
fi
