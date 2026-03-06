# Windows DB Reset Runbook (PowerShell)

This runbook is for fixing local Postgres auth/setup issues on Windows using the Dockerized DB as the single source of truth.

## Canonical local DB config

Expected local values:
- `POSTGRES_HOST=localhost`
- `POSTGRES_PORT=5432`
- `POSTGRES_DB=social_attribution_engine`
- `POSTGRES_USER=postgres`
- `POSTGRES_PASSWORD=postgres`

`POSTGRES_*` is the canonical config path in the app.

## 1) Safe reset (keeps existing Docker volume/data)

```powershell
Copy-Item .env.example .env -ErrorAction SilentlyContinue
powershell -ExecutionPolicy Bypass -File .\scripts\reset_local_db_env.ps1
```

## 2) Destructive reset (wipes Docker Postgres volume/data)

Use this when auth drift persists or DB state is corrupt.

```powershell
Copy-Item .env.example .env -ErrorAction SilentlyContinue
powershell -ExecutionPolicy Bypass -File .\scripts\reset_local_db_env.ps1 -WipeData
```

## 3) Verify DB connectivity

```powershell
python .\scripts\check_db_connection.py
```

## 4) Reinitialize schema and seed

```powershell
python .\scripts\init_db.py
python .\scripts\reset_db.py
python .\scripts\seed_data.py
```

## 5) Full pipeline verification

```powershell
python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
python .\scripts\verify_orchestration.py --simulate-failure --print-snapshot
```

## Single PowerShell Recovery Sequence

This is the **destructive** reset path.  
It wipes the Docker Postgres volume for this project and is recommended when auth mismatch persists.

```powershell
Copy-Item .env.example .env -ErrorAction SilentlyContinue
powershell -ExecutionPolicy Bypass -File .\scripts\reset_local_db_env.ps1 -WipeData
python .\scripts\check_db_connection.py
python .\scripts\init_db.py
python .\scripts\reset_db.py
python .\scripts\seed_data.py
python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
python .\scripts\verify_orchestration.py --simulate-failure --print-snapshot
```

Success checklist:
- DB connectivity check succeeded.
- schema/init/reset/seed commands succeeded.
- full pipeline run completed successfully.
- orchestration verification passed.
- no `password authentication failed` errors remain.

## Troubleshooting

`docker` not found:
- Install/start Docker Desktop.
- Restart PowerShell after Docker install.
- Confirm with:
```powershell
docker --version
docker compose version
```

Port `5432` already in use:
- Check usage:
```powershell
netstat -ano | findstr :5432
```
- Stop conflicting local Postgres service, or change `POSTGRES_PORT` in `.env` and rerun reset.

`password authentication failed for user "postgres"`:
- Most common cause is stale volume created with old credentials.
- Run destructive reset:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\reset_local_db_env.ps1 -WipeData
```
- Ensure `.env` matches `.env.example`.

`.env` not being read:
- Confirm `.env` exists in repo root:
```powershell
Get-Item .\.env
```
- Regenerate from template:
```powershell
Copy-Item .env.example .env -Force
```

Stale `DATABASE_URL` interfering:
- Current app prefers `POSTGRES_*`.
- Keep `DATABASE_URL` unset/commented in `.env` for local runs unless intentionally needed.

Container unhealthy:
- Check status/logs:
```powershell
docker compose ps
docker compose logs postgres --tail=200
```
- If still unhealthy, run destructive reset.
