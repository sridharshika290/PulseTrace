# PulseTrace

PulseTrace is a lightweight message auditing sidecar for Kafka and Redis-driven systems. It mirrors traffic, tracks message lineage, and flags delivery anomalies without requiring producer or consumer code changes.

## What is included

- FastAPI audit core with a simulator-first default
- Duplicate, out-of-order, lag spike, DLQ burst, and slow-processing detection
- Prometheus-style `/metrics` endpoint
- React dashboard with live polling and WebSocket updates
- Docker Compose stack for Kafka, Redis, API, and frontend
- Pytest coverage for the anomaly engine

## Quick start

Backend:

```bash
cd backend
python -m venv .venv
.venv\\Scripts\\activate
pip install -e .[dev]
uvicorn app.main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

The backend connects to Kafka and Redis by default. Set `SIMULATOR_MODE=true` only if you want local demo traffic without a broker feed.

## Docker Compose

```bash
docker compose up --build
```

## API

- `GET /api/summary`
- `GET /api/events`
- `GET /api/anomalies`
- `GET /api/heatmap`
- `GET /metrics`
- `WS /ws/live`

## Handoff — Run & Maintain

- Quick dev run (all services):

```bash
docker compose up --build
```

- Start only backend locally (python venv):

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
uvicorn app.main:app --reload --port 8000
```

- Frontend dev:

```bash
cd frontend
npm ci --legacy-peer-deps
npm run dev
```

- Secrets (Vault):
	- A dev Vault is included in `docker-compose.yml`. The `vault-init` job writes `secret/data/API_KEY`.
	- Production: hook real Vault and rotate tokens; `backend/app/secrets.py` reads `VAULT_ADDR` and `VAULT_TOKEN`.

- Observability:
	- OTEL Collector configured at `otel/collector-config.yaml` and exposed on ports `4317`/`4318`.
	- Prometheus config: `prometheus/prometheus.yml` (port `9090`).
	- Grafana provisioning is in `grafana/` (port `3000`). Default admin password: `admin`.

- CI / E2E:
	- GitHub Actions includes backend tests, frontend build, and Playwright E2E (`.github/workflows/ci.yml`).

- Maintaining the repo:
	- Run `pytest` in `backend/` to validate changes.
	- Update `pyproject.toml` for Python deps; use `npm` in `frontend/` for UI deps.
	- To add dashboards, place JSON in `grafana/dashboards/` and update provisioning.

