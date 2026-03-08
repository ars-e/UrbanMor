# UrbanMor Backend Scaffold

## Stack
- FastAPI
- Async SQLAlchemy + asyncpg
- Pydantic response models
- Alembic migrations
- pytest

## Quickstart

```bash
cd /Users/ars-e/projects/UrbanMor/backend
python3 -m pip install -e .[dev]
uvicorn app.main:app --reload --port 8000
```

Open docs at `http://127.0.0.1:8000/docs`.

## Environment
Copy `.env.example` to `.env` and adjust values as needed.

Useful env vars:

- `DATABASE_URL` (default: `postgresql+asyncpg:///urbanmor`)
- `CORS_ALLOW_ORIGINS` (comma-separated origins or `*`)

## Run tests

```bash
cd /Users/ars-e/projects/UrbanMor/backend
pytest
```

## Alembic

```bash
cd /Users/ars-e/projects/UrbanMor/backend
alembic upgrade head
```

## Deploy on Render (Hobby)

Render blueprint is included at:

- `/Users/ars-e/projects/UrbanMor/render.yaml`

Backend startup script used by Render:

- `/Users/ars-e/projects/UrbanMor/backend/scripts/render_start.sh`

Full deploy + database copy steps:

- `/Users/ars-e/projects/UrbanMor/backend/DEPLOY_RENDER.md`

## Profiling Scripts

```bash
# API response targets report
/Users/ars-e/projects/UrbanMor/backend/scripts/profile_response_targets.sh

# Query plans (EXPLAIN ANALYZE) report
/Users/ars-e/projects/UrbanMor/backend/scripts/profile_query_plans.sh
```

## Core endpoints

- `GET /cities`
- `GET /cities/{city}/wards`
- `GET /cities/{city}/wards/geojson`
- `GET /cities/{city}/metrics`
- `GET /cities/{city}/wards/{ward_id}`
- `POST /analyse`
- `GET /analyse/jobs/{job_id}`
- `GET /meta/metrics`
- `GET /health`
