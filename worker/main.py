import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper()


@dataclass(frozen=True)
class WorkerSettings:
    db_path: Path
    interval_seconds: int
    run_once: bool
    alpha_vantage_api_key: str
    alpha_vantage_base_url: str
    newsapi_api_key: str
    newsapi_base_url: str


def load_settings() -> WorkerSettings:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    db_path = Path(os.getenv("DB_PATH", str(data_dir / "market_observability.db")))
    run_once = os.getenv("WORKER_RUN_ONCE", "false").lower() in {"1", "true", "yes"}

    # Docs:
    # Alpha Vantage: https://www.alphavantage.co/documentation/
    # NewsAPI: https://newsapi.org/docs/endpoints/everything
    return WorkerSettings(
        db_path=db_path,
        interval_seconds=int(os.getenv("WORKER_INTERVAL_SECONDS", "300")),
        run_once=run_once,
        alpha_vantage_api_key=os.getenv("ALPHA_VANTAGE_API_KEY", "demo"),
        alpha_vantage_base_url=os.getenv(
            "ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"
        ),
        newsapi_api_key=os.getenv("NEWSAPI_API_KEY", "mock_newsapi_key"),
        newsapi_base_url=os.getenv(
            "NEWSAPI_BASE_URL", "https://newsapi.org/v2/everything"
        ),
    )


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                price REAL,
                source TEXT,
                captured_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                headline TEXT NOT NULL,
                url TEXT,
                source TEXT,
                published_at TEXT,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                summary TEXT NOT NULL,
                sentiment TEXT,
                movement_delta REAL,
                data_timestamp TEXT,
                created_at TEXT NOT NULL,
                raw_json TEXT
            )
            """
        )
        conn.commit()


def fetch_price_from_alpha_vantage(settings: WorkerSettings, ticker: str) -> tuple[float, str]:
    if settings.alpha_vantage_api_key in {"", "mock", "mock_alpha_vantage_key"}:
        return mock_price_for_ticker(ticker), "mock_alpha_vantage"

    try:
        response = requests.get(
            settings.alpha_vantage_base_url,
            params={
                "function": "GLOBAL_QUOTE",
                "symbol": ticker,
                "apikey": settings.alpha_vantage_api_key,
            },
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return mock_price_for_ticker(ticker), "mock_alpha_vantage_fallback"

    quote = payload.get("Global Quote", {})
    price_value = quote.get("05. price")
    if price_value is None:
        return mock_price_for_ticker(ticker), "mock_alpha_vantage_fallback"

    try:
        return float(price_value), "alpha_vantage"
    except ValueError:
        return mock_price_for_ticker(ticker), "mock_alpha_vantage_fallback"


def fetch_news_items(settings: WorkerSettings, ticker: str) -> tuple[list[dict[str, Any]], str]:
    if settings.newsapi_api_key in {"", "mock_newsapi_key", "mock"}:
        return mock_news_for_ticker(ticker), "mock_newsapi"

    try:
        response = requests.get(
            settings.newsapi_base_url,
            params={
                "q": ticker,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 3,
                "apiKey": settings.newsapi_api_key,
            },
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return mock_news_for_ticker(ticker), "mock_newsapi_fallback"

    articles = payload.get("articles", [])
    if not articles:
        return mock_news_for_ticker(ticker), "mock_newsapi_fallback"

    items: list[dict[str, Any]] = []
    for article in articles[:3]:
        items.append(
            {
                "headline": article.get("title") or f"{ticker} headline unavailable",
                "url": article.get("url"),
                "source": (article.get("source") or {}).get("name", "newsapi"),
                "published_at": article.get("publishedAt"),
            }
        )
    return items, "newsapi"


def mock_price_for_ticker(ticker: str) -> float:
    seed = sum(ord(char) for char in ticker)
    return round(50 + (seed % 200) + ((seed % 17) / 10), 2)


def mock_news_for_ticker(ticker: str) -> list[dict[str, Any]]:
    now = utc_now_iso()
    return [
        {
            "headline": f"{ticker} mock headline: earnings outlook in focus",
            "url": "https://example.com/mock-earnings",
            "source": "mock-news",
            "published_at": now,
        },
        {
            "headline": f"{ticker} mock headline: analyst sentiment mixed",
            "url": "https://example.com/mock-analyst",
            "source": "mock-news",
            "published_at": now,
        },
    ]


class WorkerService:
    def __init__(self, settings: WorkerSettings):
        self.settings = settings

    def run_cycle(self) -> dict[str, int]:
        init_db(self.settings.db_path)
        with sqlite3.connect(self.settings.db_path) as conn:
            conn.row_factory = sqlite3.Row
            watchlist = [
                normalize_ticker(row["ticker"])
                for row in conn.execute(
                    "SELECT ticker FROM watchlist ORDER BY ticker ASC"
                ).fetchall()
            ]

            analyses_written = 0
            news_written = 0
            snapshots_written = 0

            for ticker in watchlist:
                analysis_result = self._process_ticker(conn, ticker)
                analyses_written += analysis_result["analyses"]
                news_written += analysis_result["news"]
                snapshots_written += analysis_result["snapshots"]

            conn.commit()

        return {
            "tickers_processed": len(watchlist),
            "analyses_written": analyses_written,
            "news_written": news_written,
            "snapshots_written": snapshots_written,
        }

    def _process_ticker(self, conn: sqlite3.Connection, ticker: str) -> dict[str, int]:
        now = utc_now_iso()
        price, price_source = fetch_price_from_alpha_vantage(self.settings, ticker)
        news_items, news_source = fetch_news_items(self.settings, ticker)

        prev_row = conn.execute(
            """
            SELECT price
            FROM price_snapshots
            WHERE ticker = ?
            ORDER BY captured_at DESC, id DESC
            LIMIT 1
            """,
            (ticker,),
        ).fetchone()
        previous_price = prev_row["price"] if prev_row else None
        movement_delta = (
            round(price - float(previous_price), 4) if previous_price is not None else None
        )

        conn.execute(
            """
            INSERT INTO price_snapshots (ticker, price, source, captured_at)
            VALUES (?, ?, ?, ?)
            """,
            (ticker, price, price_source, now),
        )

        for item in news_items:
            conn.execute(
                """
                INSERT INTO news_items (ticker, headline, url, source, published_at, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    item["headline"],
                    item.get("url"),
                    item.get("source", news_source),
                    item.get("published_at"),
                    now,
                ),
            )

        summary = build_summary(ticker, price, movement_delta, news_items)
        sentiment = movement_to_sentiment(movement_delta)
        raw_json = json.dumps(
            {
                "ticker": ticker,
                "price": price,
                "price_source": price_source,
                "news_source": news_source,
                "headlines": [item["headline"] for item in news_items],
            }
        )
        conn.execute(
            """
            INSERT INTO analyses (ticker, summary, sentiment, movement_delta, data_timestamp, created_at, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ticker, summary, sentiment, movement_delta, now, now, raw_json),
        )

        print(
            f"[worker] processed ticker={ticker} price={price} source={price_source} "
            f"news_items={len(news_items)}",
            flush=True,
        )
        return {"analyses": 1, "news": len(news_items), "snapshots": 1}


def build_summary(
    ticker: str, price: float, movement_delta: float | None, news_items: list[dict[str, Any]]
) -> str:
    movement_text = (
        "no prior snapshot available"
        if movement_delta is None
        else f"delta since last snapshot: {movement_delta:+.4f}"
    )
    headline_text = "; ".join(item["headline"] for item in news_items[:2])
    if not headline_text:
        headline_text = "no recent headlines found"
    return (
        f"{ticker} last price {price:.2f}; {movement_text}. "
        f"Top headlines: {headline_text}."
    )


def movement_to_sentiment(delta: float | None) -> str:
    if delta is None:
        return "neutral"
    if delta > 0:
        return "positive"
    if delta < 0:
        return "negative"
    return "neutral"


def main() -> None:
    settings = load_settings()
    service = WorkerService(settings)

    while True:
        started = utc_now_iso()
        print(f"[worker] cycle started at {started}", flush=True)
        result = service.run_cycle()
        print(f"[worker] cycle result={result}", flush=True)

        if settings.run_once:
            break

        time.sleep(settings.interval_seconds)


if __name__ == "__main__":
    main()
