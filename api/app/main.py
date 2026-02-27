import json
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from app.db import get_conn, init_db, seed_watchlist, utc_now_iso
from app.settings import Settings, load_settings

HTTP_REQUESTS_TOTAL = Counter(
    "moa_http_requests_total",
    "Total HTTP requests handled by the API",
    ["method", "route", "status_code"],
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "moa_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "route"],
)
HTTP_EXCEPTIONS_TOTAL = Counter(
    "moa_http_exceptions_total",
    "Total unhandled exceptions in API request processing",
    ["method", "route", "exception_type"],
)
WATCHLIST_MUTATIONS_TOTAL = Counter(
    "moa_watchlist_mutations_total",
    "Watchlist mutations grouped by action",
    ["action"],
)
DEFAULT_TICKERS = ["AAPL", "MSFT", "TSLA"]


class WatchlistUpsertRequest(BaseModel):
    ticker: str = Field(min_length=1, max_length=10)


def pagination(page: int, limit: int) -> tuple[int, int, int]:
    safe_page = max(page, 1)
    safe_limit = min(max(limit, 1), 100)
    offset = (safe_page - 1) * safe_limit
    return safe_page, safe_limit, offset


def normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper()


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(title="Market Observability Agent API")
    app.state.settings = settings or load_settings()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def prometheus_metrics_middleware(
        request: Request, call_next: Any
    ) -> Response:
        started = time.perf_counter()
        method = request.method
        route = request.url.path
        status_code = "500"
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            route_obj = request.scope.get("route")
            if route_obj is not None and hasattr(route_obj, "path"):
                route = str(route_obj.path)
            return response
        except Exception as exc:
            route_obj = request.scope.get("route")
            if route_obj is not None and hasattr(route_obj, "path"):
                route = str(route_obj.path)
            HTTP_EXCEPTIONS_TOTAL.labels(
                method=method,
                route=route,
                exception_type=type(exc).__name__,
            ).inc()
            raise
        finally:
            elapsed = time.perf_counter() - started
            HTTP_REQUEST_DURATION_SECONDS.labels(
                method=method,
                route=route,
            ).observe(elapsed)
            HTTP_REQUESTS_TOTAL.labels(
                method=method,
                route=route,
                status_code=status_code,
            ).inc()

    @app.on_event("startup")
    def startup() -> None:
        cfg: Settings = app.state.settings
        init_db(cfg.db_path)
        seed_watchlist(cfg.db_path, DEFAULT_TICKERS)

    @app.get("/health")
    def health() -> dict[str, str]:
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    @app.get("/watchlist")
    def watchlist() -> dict[str, list[str]]:
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            rows = conn.execute(
                "SELECT ticker FROM watchlist ORDER BY ticker ASC"
            ).fetchall()
        return {"tickers": [row["ticker"] for row in rows]}

    @app.post("/watchlist", status_code=201)
    def add_watchlist_ticker(payload: WatchlistUpsertRequest) -> dict[str, str]:
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
                    WATCHLIST_MUTATIONS_TOTAL.labels(action="add_conflict").inc()
                    raise HTTPException(
                        status_code=409, detail=f"Ticker {ticker} already exists"
                    ) from exc
                raise
        WATCHLIST_MUTATIONS_TOTAL.labels(action="add").inc()
        return {"ticker": ticker, "status": "added"}

    @app.delete("/watchlist/{ticker}")
    def remove_watchlist_ticker(ticker: str) -> dict[str, str]:
        normalized = normalize_ticker(ticker)
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            result = conn.execute(
                "DELETE FROM watchlist WHERE ticker = ?", (normalized,)
            )
            conn.commit()
        if result.rowcount == 0:
            WATCHLIST_MUTATIONS_TOTAL.labels(action="remove_not_found").inc()
            raise HTTPException(status_code=404, detail=f"Ticker {normalized} not found")
        WATCHLIST_MUTATIONS_TOTAL.labels(action="remove").inc()
        return {"ticker": normalized, "status": "removed"}

    @app.get("/latest/{ticker}")
    def latest(ticker: str) -> dict[str, Any]:
        normalized = normalize_ticker(ticker)
        cfg: Settings = app.state.settings
        with get_conn(cfg.db_path) as conn:
            row = conn.execute(
                """
                SELECT ticker, summary, sentiment, movement_delta, data_timestamp, created_at, raw_json
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

        raw_payload: dict[str, Any] = {}
        try:
            raw_payload = json.loads(row["raw_json"] or "{}")
        except Exception:
            raw_payload = {}

        llm_result = raw_payload.get("llm_result") or {}
        return {
            "ticker": row["ticker"],
            "summary": row["summary"],
            "sentiment": row["sentiment"],
            "movement_delta": row["movement_delta"],
            "data_timestamp": row["data_timestamp"],
            "created_at": row["created_at"],
            "hypothesis": raw_payload.get("hypothesis"),
            "llm_triggered": raw_payload.get("llm_triggered"),
            "trigger_reason": raw_payload.get("trigger_reason"),
            "valid_json": raw_payload.get("valid_json"),
            "confidence": llm_result.get("confidence"),
            "counterpoints": llm_result.get("counterpoints"),
            "limitations": llm_result.get("limitations"),
        }

    @app.get("/history/{ticker}")
    def history(ticker: str, limit: int = 20) -> dict[str, Any]:
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

    @app.get("/prices/{ticker}")
    def prices(ticker: str, page: int = 1, limit: int = 50) -> dict[str, Any]:
        normalized = normalize_ticker(ticker)
        safe_page, safe_limit, offset = pagination(page, limit)
        cfg: Settings = app.state.settings

        with get_conn(cfg.db_path) as conn:
            total = conn.execute(
                "SELECT COUNT(*) AS total FROM price_snapshots WHERE ticker = ?",
                (normalized,),
            ).fetchone()["total"]
            rows = conn.execute(
                """
                SELECT ticker, price, source, captured_at
                FROM price_snapshots
                WHERE ticker = ?
                ORDER BY captured_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (normalized, safe_limit, offset),
            ).fetchall()

        return {
            "ticker": normalized,
            "page": safe_page,
            "limit": safe_limit,
            "total": total,
            "has_next": offset + len(rows) < total,
            "items": [dict(row) for row in rows],
        }

    @app.get("/news/{ticker}")
    def news(ticker: str, page: int = 1, limit: int = 20) -> dict[str, Any]:
        normalized = normalize_ticker(ticker)
        safe_page, safe_limit, offset = pagination(page, limit)
        cfg: Settings = app.state.settings

        with get_conn(cfg.db_path) as conn:
            total = conn.execute(
                "SELECT COUNT(*) AS total FROM news_items WHERE ticker = ?",
                (normalized,),
            ).fetchone()["total"]
            rows = conn.execute(
                """
                SELECT ticker, headline, url, source, published_at, fetched_at
                FROM news_items
                WHERE ticker = ?
                ORDER BY fetched_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (normalized, safe_limit, offset),
            ).fetchall()

        return {
            "ticker": normalized,
            "page": safe_page,
            "limit": safe_limit,
            "total": total,
            "has_next": offset + len(rows) < total,
            "items": [dict(row) for row in rows],
        }

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.exception_handler(Exception)
    def unhandled_error(_: Any, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error"},
        )

    return app


app = create_app()
