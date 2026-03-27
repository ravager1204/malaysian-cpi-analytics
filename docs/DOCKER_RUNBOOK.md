# Docker Runbook

This project now supports two local container workflows:

## 1. PostgreSQL Only

Use this when you want the fastest end-to-end local run with Python scripts.

```powershell
docker compose up -d
venv\Scripts\python.exe scripts\init_database.py
venv\Scripts\python.exe scripts\run_full_pipeline.py
venv\Scripts\python.exe scripts\validate_pipeline.py
```

Default host mappings:

- PostgreSQL: `localhost:5433`

## 2. PostgreSQL + Airflow

Use this when you want to demo orchestration from Docker as part of the portfolio story.

```powershell
docker compose --profile airflow up -d --build
```

Default host mappings:

- PostgreSQL: `localhost:5433`
- Airflow web UI: `http://localhost:8082`

Default Airflow login values come from `.env` / `.env.example`:

- Username: `admin`
- Password: `admin`

## Container Design

The stack is intentionally isolated from other local projects:

- Compose project name: `malaysian-cpi-analytics`
- PostgreSQL container: `cpi-postgres`
- Redis container: `cpi-redis`
- Airflow webserver: `cpi-airflow-webserver`
- Airflow scheduler: `cpi-airflow-scheduler`

## Notes

- Local Python scripts use `DB_HOST=localhost` and `DB_PORT=5433`.
- Airflow containers override the database host internally to `postgres`.
- S3 upload is disabled locally by default with `ENABLE_S3_UPLOAD=false`.
- If you already have another Postgres container on `5432`, this project will not conflict with it.
