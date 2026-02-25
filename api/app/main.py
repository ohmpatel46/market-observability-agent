from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from app.db import get_conn, init_db, seed_watchlist, utc_now_iso
from app.settings import Settings, load_settings

REQUESTS = Counter(
    "http_requests_total", "Total HTTP requests processed by endpoint", ["endpoint"]
)
DEFAULT_TICKERS = ["AAPL", "MSFT", "TSLA"]


class WatchlistUpsertRequest(BaseModel):
    ticker: str = Field(min_length=1, max_length=10)


def normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper()


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(title="Market Observability Agent API")
    app.state.settings = settings or load_settings()

    @app.on_event("startup")
    def startup() -> None:
        cfg: Settings = app.state.settings
        init_db(cfg.db_path)
        seed_watchlist(cfg.db_path, DEFAULT_TICKERS)

    @app.get("/health")
    def health() -> dict[str, str]:
        REQUESTS.labels(endpoint="health").inc()
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    @app.get("/watchlist")
    def watchlist() -> dict[str, list[str]]:
        REQUESTS.labels(endpoint="watchlist").inc()
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            rows = conn.execute(
                "SELECT ticker FROM watchlist ORDER BY ticker ASC"
            ).fetchall()
        return {"tickers": [row["ticker"] for row in rows]}

    @app.post("/watchlist", status_code=201)
    def add_watchlist_ticker(payload: WatchlistUpsertRequest) -> dict[str, str]:
        REQUESTS.labels(endpoint="watchlist_post").inc()
        ticker = normalize_ticker(payload.ticker)
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            try:
                conn.execute(
                    "INSERT INTO watchlist (ticker, created_at) VALUES (?, ?)",
                    (ticker, utc_now_iso()),
                )
                conn.commit()
            except Exception as exc:
                if "UNIQUE constraint failed" in str(exc):
                    raise HTTPException(
                        status_code=409, detail=f"Ticker {ticker} already exists"
                    ) from exc
                raise
        return {"ticker": ticker, "status": "added"}

    @app.delete("/watchlist/{ticker}")
    def remove_watchlist_ticker(ticker: str) -> dict[str, str]:
        REQUESTS.labels(endpoint="watchlist_delete").inc()
        normalized = normalize_ticker(ticker)
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            result = conn.execute(
                "DELETE FROM watchlist WHERE ticker = ?", (normalized,)
            )
            conn.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Ticker {normalized} not found")
        return {"ticker": normalized, "status": "removed"}

    @app.get("/latest/{ticker}")
    def latest(ticker: str) -> dict[str, Any]:
        REQUESTS.labels(endpoint="latest").inc()
        normalized = normalize_ticker(ticker)
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            row = conn.execute(
                """
                SELECT ticker, summary, sentiment, movement_delta, data_timestamp, created_at
                FROM analyses
                WHERE ticker = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()

        if row is None:
            return {
                "ticker": normalized,
                "status": "pending",
                "message": "No analysis stored yet. Worker population starts in Task 3.",
            }

        return {
            "ticker": row["ticker"],
            "summary": row["summary"],
            "sentiment": row["sentiment"],
            "movement_delta": row["movement_delta"],
            "data_timestamp": row["data_timestamp"],
            "created_at": row["created_at"],
        }

    @app.get("/history/{ticker}")
    def history(ticker: str, limit: int = 20) -> dict[str, Any]:
        REQUESTS.labels(endpoint="history").inc()
        normalized = normalize_ticker(ticker)
        safe_limit = min(max(limit, 1), 100)
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            rows = conn.execute(
                """
                SELECT ticker, summary, sentiment, movement_delta, data_timestamp, created_at
                FROM analyses
                WHERE ticker = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (normalized, safe_limit),
            ).fetchall()

        return {
            "ticker": normalized,
            "items": [dict(row) for row in rows],
            "count": len(rows),
        }

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/config/external-sources")
    def external_sources() -> dict[str, str]:
        REQUESTS.labels(endpoint="config_external_sources").inc()
        cfg: Settings = app.state.settings
        return {
            "alpha_vantage_base_url": cfg.alpha_vantage_base_url,
            "alpha_vantage_api_key": cfg.alpha_vantage_api_key,
            "newsapi_base_url": cfg.newsapi_base_url,
            "newsapi_api_key": cfg.newsapi_api_key,
        }

    @app.exception_handler(Exception)
    def unhandled_error(_: Any, exc: Exception) -> JSONResponse:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    return app


app = create_app()
