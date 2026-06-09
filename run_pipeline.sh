#!/bin/bash
# Daily pipeline runner — scraper → silver → gold
# Each stage runs in an isolated Docker container removed on completion (--rm).
# Intended for unattended execution via Linux cron.

set -e  # abort immediately if any stage fails

# cron runs in a minimal environment without the user's PATH configured.
# Declare Docker's location explicitly so the script works in all contexts.
export PATH="/usr/bin:/usr/local/bin:/bin:$PATH"

# --- Paths ---
# cd + pwd resolves the absolute path regardless of where the script is invoked from.
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/pipeline_$TODAY.log"

# -p silently succeeds if the directory already exists
mkdir -p "$LOG_DIR"

# --- Helper: print a timestamped message and append it to the daily log ---
log() {
    echo "[$(date +%H:%M:%S)] $1" | tee -a "$LOG_FILE"
}

# --- Start ---
log "============================================"
log "Pipeline started — $TODAY"
log "============================================"

# --- Stage 1: Scraper (Bronze) ---
log "STAGE 1/3 — Scraper starting..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" run --rm scraper >> "$LOG_FILE" 2>&1
log "STAGE 1/3 — Scraper finished OK"

# --- Stage 2: Silver ---
log "STAGE 2/3 — Silver transform starting..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" run --rm transform >> "$LOG_FILE" 2>&1
log "STAGE 2/3 — Silver finished OK"

# --- Stage 3: Gold ---
log "STAGE 3/3 — Gold aggregation starting..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" run --rm gold >> "$LOG_FILE" 2>&1
log "STAGE 3/3 — Gold finished OK"

# --- Done ---
log "============================================"
log "Pipeline complete. Data saved to $PROJECT_DIR/data/"
log "============================================"