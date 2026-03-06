#!/usr/bin/env bash
set -euo pipefail

WIPE_DATA="${1:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker CLI was not found in PATH. Install/start Docker Desktop and retry." >&2
  exit 1
fi

if [[ ! -f ".env" ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

echo "Stopping Docker Compose services..."
if [[ "$WIPE_DATA" == "--wipe-data" ]]; then
  echo "Destructive reset enabled: removing Docker volumes (this wipes local DB data)."
  docker compose down -v
else
  docker compose down
fi

echo "Starting Postgres container with project-local credentials..."
docker compose up -d

echo
echo "Next steps:"
echo "1) python scripts/check_db_connection.py"
echo "2) python scripts/init_db.py"
echo "3) python scripts/seed_data.py"
echo "4) python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild"
