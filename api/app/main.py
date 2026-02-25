from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

app = FastAPI(title="Market Observability Agent API")

REQUESTS = Counter("http_requests_total", "Total HTTP requests processed by endpoint", ["endpoint"])
WATCHLIST = ["AAPL", "MSFT", "TSLA"]


@app.get("/health")
def health() -> dict[str, str]:
    REQUESTS.labels(endpoint="health").inc()
    return {"status": "ok"}


@app.get("/watchlist")
def watchlist() -> dict[str, list[str]]:
    REQUESTS.labels(endpoint="watchlist").inc()
    return {"tickers": WATCHLIST}


@app.get("/latest/{ticker}")
def latest(ticker: str) -> dict[str, Any]:
    REQUESTS.labels(endpoint="latest").inc()
    return {
        "ticker": ticker.upper(),
        "status": "pending",
        "message": "No analysis stored yet. Implemented in Task 2/3.",
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.exception_handler(Exception)
def unhandled_error(_: Any, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=500, content={"error": str(exc)})