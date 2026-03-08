# UrbanMor Frontend

React + TypeScript + Vite app for:

- city switching and ward rendering on MapLibre
- ward hover/select and multi-select UX
- city choropleth by metric id
- click-to-analysis for ward details
- custom polygon analysis with async job polling/progress
- geometry guardrails (self-intersection/invalid/too-small checks)
- metadata-driven grouped metric panel with quality badges
- reference delta view (city average or selected ward)
- two-area compare mode (ward or polygon snapshots)
- JSON/CSV metric export actions

## Requirements

- Node.js 20+
- UrbanMor backend running on `http://127.0.0.1:8000` (or set `VITE_API_BASE_URL`)

## Local run

```bash
cp .env.example .env
npm install
npm run dev
```

Open: `http://127.0.0.1:5173`

## Build

```bash
npm run build
npm run preview
```

## Key API usage

- `GET /cities`
- `GET /cities/{city}/wards`
- `GET /cities/{city}/wards/geojson`
- `GET /cities/{city}/wards/{ward_id}`
- `POST /analyse` (`mode=wards`, `mode=custom_polygon`)
- `GET /analyse/jobs/{job_id}`
- `GET /meta/metrics`
