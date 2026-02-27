# Market Observability Agent (MVP)

## Task 1 Scaffold

### Prerequisites
- Docker Desktop (or Docker Engine + Compose)

### Boot the stack
```bash
docker compose up --build
```

### Endpoints
- API health: `http://localhost:8000/health`
- Watchlist: `GET/POST/DELETE http://localhost:8000/watchlist`
- Latest analysis: `http://localhost:8000/latest/AAPL`
- History: `http://localhost:8000/history/AAPL`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

### Run API tests
```bash
docker compose run --rm api pytest -q
```

### Run Worker tests
```bash
docker compose run --rm --build worker pytest -q
```

### Trigger one worker cycle manually
```bash
docker compose run --rm -e WORKER_RUN_ONCE=true worker
```

### External API setup (Task 3-ready)
- `ALPHA_VANTAGE_API_KEY` default is `demo`
- `NEWSAPI_API_KEY` default is `mock_newsapi_key`
- Docs:
  - Alpha Vantage: `https://www.alphavantage.co/documentation/`
  - NewsAPI Everything: `https://newsapi.org/docs/endpoints/everything`

### Stop
```bash
docker compose down
```
