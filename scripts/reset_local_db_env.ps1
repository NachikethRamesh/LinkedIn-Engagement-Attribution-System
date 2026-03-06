param(
    [switch]$WipeData
)

$ErrorActionPreference = "Stop"

if (!(Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI was not found in PATH. Install/start Docker Desktop and retry."
}

if (!(Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

Write-Host "Stopping Docker Compose services..."
if ($WipeData) {
    Write-Host "Destructive reset enabled: removing Docker volumes (this wipes local DB data)."
    docker compose down -v
} else {
    docker compose down
}

Write-Host "Starting Postgres container with project-local credentials..."
docker compose up -d

Write-Host ""
Write-Host "Next steps:"
Write-Host "1) python scripts/check_db_connection.py"
Write-Host "2) python scripts/init_db.py"
Write-Host "3) python scripts/seed_data.py"
Write-Host "4) python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild"
