import json
import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import requests
from pydantic import BaseModel, ValidationError, field_validator


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
    langfuse_public_key: str
    langfuse_secret_key: str
    langfuse_base_url: str


class NewsItem(BaseModel):
    headline: str
    url: str | None = None
    source: str
    published_at: str | None = None

    @field_validator("headline")
    @classmethod
    def validate_headline(cls, value: str) -> str:
        headline = value.strip()
        if not headline:
            raise ValueError("headline cannot be empty")
        return headline

    @field_validator("url", "published_at")
    @classmethod
    def normalize_optional_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

    @field_validator("source")
    @classmethod
    def normalize_source(cls, value: str) -> str:
        source = value.strip()
        return source or "unknown-source"


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
        langfuse_public_key=os.getenv("LANGFUSE_PUBLIC_KEY", ""),
        langfuse_secret_key=os.getenv("LANGFUSE_SECRET_KEY", ""),
        langfuse_base_url=os.getenv("LANGFUSE_BASE_URL", "https://cloud.langfuse.com"),
    )


class NoopObservation:
    def update(self, **_: Any) -> None:
        return None


class LangfuseTracer:
    def __init__(self, settings: WorkerSettings):
        self.enabled = False
        self._client: Any = None
        if not self._has_credentials(settings):
            return

        try:
            from langfuse import get_client
        except Exception:
            return

        try:
            self._client = get_client(
                public_key=settings.langfuse_public_key,
                secret_key=settings.langfuse_secret_key,
                base_url=settings.langfuse_base_url,
            )
            self.enabled = True
        except Exception:
            self.enabled = False
            self._client = None

    @staticmethod
    def _has_credentials(settings: WorkerSettings) -> bool:
        bad_public = {"", "mock", "demo", "your_langfuse_public_key"}
        bad_secret = {"", "mock", "demo", "your_langfuse_secret_key"}
        return (
            settings.langfuse_public_key not in bad_public
            and settings.langfuse_secret_key not in bad_secret
        )

    @contextmanager
    def observation(
        self, name: str, *, as_type: str = "span", **kwargs: Any
    ) -> Generator[Any, None, None]:
        if not self.enabled or self._client is None:
            yield NoopObservation()
            return

        with self._client.start_as_current_observation(
            name=name, as_type=as_type, **kwargs
        ) as obs:
            yield obs

    def flush(self) -> None:
        if not self.enabled or self._client is None:
            return
        try:
            self._client.flush()
        except Exception:
            return


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
            CREATE UNIQUE INDEX IF NOT EXISTS idx_news_items_dedupe
            ON news_items (ticker, headline, IFNULL(url, ''), IFNULL(published_at, ''))
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


def fetch_news_items(settings: WorkerSettings, ticker: str) -> tuple[list[NewsItem], str]:
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

    raw_items: list[dict[str, Any]] = []
    for article in articles[:6]:
        raw_items.append(
            {
                "headline": article.get("title") or f"{ticker} headline unavailable",
                "url": article.get("url"),
                "source": (article.get("source") or {}).get("name", "newsapi"),
                "published_at": article.get("publishedAt"),
            }
        )
    parsed_items = parse_and_dedupe_news_items(raw_items)
    return parsed_items[:3], "newsapi"


def mock_price_for_ticker(ticker: str) -> float:
    seed = sum(ord(char) for char in ticker)
    return round(50 + (seed % 200) + ((seed % 17) / 10), 2)


def mock_news_for_ticker(ticker: str) -> list[NewsItem]:
    now = utc_now_iso()
    return parse_and_dedupe_news_items(
        [
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
    )


def parse_and_dedupe_news_items(raw_items: list[dict[str, Any]]) -> list[NewsItem]:
    deduped: list[NewsItem] = []
    seen: set[tuple[str, str, str]] = set()

    for raw in raw_items:
        try:
            item = NewsItem.model_validate(raw)
        except ValidationError:
            continue

        key = (
            item.headline.casefold(),
            item.url or "",
            item.published_at or "",
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


class WorkerService:
    def __init__(self, settings: WorkerSettings, tracer: LangfuseTracer | None = None):
        self.settings = settings
        self.tracer = tracer or LangfuseTracer(settings)

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

            with self.tracer.observation(
                name="worker-cycle",
                as_type="span",
                input={"watchlist_size": len(watchlist), "tickers": watchlist},
            ) as cycle_span:
                for ticker in watchlist:
                    analysis_result = self._process_ticker(conn, ticker)
                    analyses_written += analysis_result["analyses"]
                    news_written += analysis_result["news"]
                    snapshots_written += analysis_result["snapshots"]

                cycle_result = {
                    "tickers_processed": len(watchlist),
                    "analyses_written": analyses_written,
                    "news_written": news_written,
                    "snapshots_written": snapshots_written,
                }
                cycle_span.update(output=cycle_result)

            conn.commit()
        self.tracer.flush()

        return cycle_result

    def _process_ticker(self, conn: sqlite3.Connection, ticker: str) -> dict[str, int]:
        with self.tracer.observation(
            name="process-ticker",
            as_type="span",
            input={"ticker": ticker},
        ) as ticker_span:
            now = utc_now_iso()
            with self.tracer.observation(
                name="fetch_price",
                as_type="tool",
                input={"ticker": ticker, "provider": "alpha_vantage"},
            ) as fetch_price_span:
                price, price_source = fetch_price_from_alpha_vantage(self.settings, ticker)
                fetch_price_span.update(
                    output={"price": price, "source": price_source},
                )

            with self.tracer.observation(
                name="fetch_news",
                as_type="tool",
                input={"ticker": ticker, "provider": "newsapi"},
            ) as fetch_news_span:
                news_items, news_source = fetch_news_items(self.settings, ticker)
                fetch_news_span.update(
                    output={
                        "source": news_source,
                        "count": len(news_items),
                        "headlines": [item.headline for item in news_items[:3]],
                    }
                )

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

            inserted_news_items = 0
            for item in news_items:
                result = conn.execute(
                    """
                    INSERT OR IGNORE INTO news_items (ticker, headline, url, source, published_at, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ticker,
                        item.headline,
                        item.url,
                        item.source or news_source,
                        item.published_at,
                        now,
                    ),
                )
                inserted_news_items += result.rowcount

            with self.tracer.observation(
                name="summarize",
                as_type="generation",
                model="rule-based-summary-v1",
                input={
                    "ticker": ticker,
                    "price": price,
                    "movement_delta": movement_delta,
                    "headlines": [item.headline for item in news_items[:3]],
                },
            ) as summarize_span:
                summary = build_summary(ticker, price, movement_delta, news_items)
                sentiment = movement_to_sentiment(movement_delta)
                summarize_span.update(
                    output={
                        "summary": summary,
                        "sentiment": sentiment,
                        "movement_delta": movement_delta,
                    }
                )

            hypothesis_text = build_hypothesis(ticker, movement_delta, news_items)
            with self.tracer.observation(
                name="hypothesis",
                as_type="span",
                input={"ticker": ticker},
            ) as hypothesis_span:
                hypothesis_span.update(
                    output={"hypothesis": hypothesis_text},
                    metadata={
                        "grounded_headline_used": len(news_items) > 0,
                        "valid_json": True,
                    },
                )

            raw_json = json.dumps(
                {
                    "ticker": ticker,
                    "price": price,
                    "price_source": price_source,
                    "news_source": news_source,
                    "headlines": [item.headline for item in news_items],
                    "hypothesis": hypothesis_text,
                }
            )
            conn.execute(
                """
                INSERT INTO analyses (ticker, summary, sentiment, movement_delta, data_timestamp, created_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    summary,
                    sentiment,
                    movement_delta,
                    now,
                    now,
                    raw_json,
                ),
            )

            ticker_span.update(
                output={
                    "ticker": ticker,
                    "price": price,
                    "sentiment": sentiment,
                    "movement_delta": movement_delta,
                    "news_items": len(news_items),
                }
            )

            print(
                f"[worker] processed ticker={ticker} price={price} source={price_source} "
                f"news_items={inserted_news_items}",
                flush=True,
            )
            return {"analyses": 1, "news": inserted_news_items, "snapshots": 1}


def build_summary(
    ticker: str, price: float, movement_delta: float | None, news_items: list[NewsItem]
) -> str:
    movement_text = (
        "no prior snapshot available"
        if movement_delta is None
        else f"delta since last snapshot: {movement_delta:+.4f}"
    )
    headline_text = "; ".join(item.headline for item in news_items[:2])
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


def build_hypothesis(
    ticker: str, movement_delta: float | None, news_items: list[NewsItem]
) -> str:
    if movement_delta is None:
        movement_text = "insufficient trend data for directional inference"
    elif movement_delta > 0:
        movement_text = "recent movement is positive"
    elif movement_delta < 0:
        movement_text = "recent movement is negative"
    else:
        movement_text = "recent movement is flat"

    headline_hint = news_items[0].headline if news_items else "no headline signal available"
    return f"{ticker}: {movement_text}; key narrative signal: {headline_hint}."


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
