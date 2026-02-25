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
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

### Stop
```bash
docker compose down
```