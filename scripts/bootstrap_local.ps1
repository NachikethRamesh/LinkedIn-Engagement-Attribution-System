$ErrorActionPreference = "Stop"

if (!(Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python is required but was not found in PATH."
}

if (!(Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

python -m pip install -r requirements.txt
python scripts\init_db.py
python scripts\seed_data.py
python scripts\inspect_tables.py