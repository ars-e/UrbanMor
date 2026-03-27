# Deploy UrbanMor Backend on Render (Hobby)

This deploy path keeps your frontend on Vercel (`/umv1`) and hosts the API + Postgres on Render.

## 1) Push code to GitHub

Render deploys from GitHub. Make sure these files are committed:

- `/Users/ars-e/projects/UrbanMor/render.yaml`
- `/Users/ars-e/projects/UrbanMor/backend/scripts/render_start.sh`
- `/Users/ars-e/projects/UrbanMor/backend/app/core/config.py`

## 2) Create Render services from Blueprint

1. In Render, click **New +** -> **Blueprint**.
2. Select the UrbanMor GitHub repo (the one containing `render.yaml`).
3. Render will create:
   - `urbanmor-api` (Python web service)
   - `urbanmor-db` (Postgres)
4. Keep the default plans from `render.yaml` unless you intentionally change them.

Current blueprint plans:

- Web: `starter`
- Postgres: `basic-256mb`

## 3) Copy your local `urbanmor` database to Render

Render starts with an empty DB. UrbanMor needs your prepared schemas/data/cache.

### 3a) Get Render External DB URL

In Render dashboard:

- `urbanmor-db` -> **Connect** -> **External Database URL**

Set it in shell:

```bash
export RENDER_DATABASE_URL='postgres://...'
```

### 3b) Dump local DB

```bash
cd /Users/ars-e/projects/UrbanMor
mkdir -p output/deploy
pg_dump --format=custom --no-owner --no-privileges --dbname=urbanmor --file=output/deploy/urbanmor.dump
```

### 3c) Restore into Render DB

```bash
cd /Users/ars-e/projects/UrbanMor
pg_restore --clean --if-exists --no-owner --no-privileges --dbname="$RENDER_DATABASE_URL" output/deploy/urbanmor.dump
```

## 4) Verify backend

Use your Render API URL from the web service (example shown):

```bash
curl -sS https://urbanmor-api.onrender.com/health
curl -sS https://urbanmor-api.onrender.com/cities
```

Expected: JSON responses (not 404/500).

## 5) Point frontend (`inkletlab.com/umv1`) to Render API

```bash
cd /Users/ars-e/projects/inklet-lab-site
VITE_API_BASE_URL="https://urbanmor-api.onrender.com" npm run sync:umv1
vercel --prod --yes
```

## 6) Final verification

```bash
curl -I https://www.inkletlab.com/umv1/
curl -sS https://urbanmor-api.onrender.com/health
```

Note: if you do not configure `/umv1-api` reverse proxy in Vercel, frontend still works by calling the full Render URL directly.

## Notes

- Render supplies DB URLs as `postgres://...`; backend now auto-normalizes this to `postgresql+asyncpg://...`.
- If you switch web service plan to `free`, expect cold starts.
- If you switch Postgres plan to `free`, Render can expire the DB after inactivity; avoid that for persistent production data.
