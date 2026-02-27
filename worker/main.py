import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Literal

import requests
from pydantic import BaseModel, Field, ValidationError, field_validator


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
    gemini_api_key: str
    gemini_model: str
    llm_price_change_threshold_pct: float
    llm_max_headlines: int
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


class EvidenceItem(BaseModel):
    headline: str
    rationale: str


class GeminiReasoning(BaseModel):
    summary: str
    sentiment: Literal["positive", "neutral", "negative"]
    confidence: float = Field(ge=0.0, le=1.0)
    hypothesis: str
    evidence: list[EvidenceItem] = Field(default_factory=list)
    counterpoints: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    grounded: bool = False


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
        gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-1.5-flash"),
        llm_price_change_threshold_pct=float(
            os.getenv("LLM_PRICE_CHANGE_THRESHOLD_PCT", "0.5")
        ),
        llm_max_headlines=int(os.getenv("LLM_MAX_HEADLINES", "5")),
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


def price_change_pct(current_price: float, previous_price: float | None) -> float | None:
    if previous_price is None or previous_price == 0:
        return None
    return round(((current_price - float(previous_price)) / float(previous_price)) * 100, 4)


def should_run_llm(
    movement_pct: float | None, inserted_news_count: int, threshold_pct: float
) -> tuple[bool, str]:
    price_trigger = movement_pct is not None and abs(movement_pct) >= threshold_pct
    news_trigger = inserted_news_count > 0

    if price_trigger and news_trigger:
        return True, "price_change_and_news_update"
    if price_trigger:
        return True, "price_change"
    if news_trigger:
        return True, "news_update"
    return False, "none"


def has_valid_gemini_key(api_key: str) -> bool:
    return api_key.strip() not in {"", "mock", "demo", "your_gemini_api_key"}


def build_gemini_prompt(payload: dict[str, Any]) -> str:
    return (
        "You are a market observability analysis assistant.\n"
        "Use only provided data and headlines. Do not invent facts.\n"
        "Avoid investment advice. Be explicit about uncertainty.\n"
        "Return strict JSON with keys:\n"
        "summary, sentiment, confidence, hypothesis, evidence, counterpoints, limitations, grounded\n"
        "where evidence is an array of {headline, rationale}.\n\n"
        f"INPUT:\n{json.dumps(payload, ensure_ascii=True)}"
    )


def extract_json_object(text: str) -> str:
    stripped = text.strip()
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", stripped, flags=re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    return stripped


def generate_gemini_reasoning(
    settings: WorkerSettings, payload: dict[str, Any]
) -> tuple[GeminiReasoning | None, bool]:
    if not has_valid_gemini_key(settings.gemini_api_key):
        return None, False

    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{settings.gemini_model}:generateContent"
    )
    request_body = {
        "contents": [{"role": "user", "parts": [{"text": build_gemini_prompt(payload)}]}],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }

    try:
        response = requests.post(
            endpoint,
            params={"key": settings.gemini_api_key},
            json=request_body,
            timeout=20,
        )
        response.raise_for_status()
        response_payload = response.json()
        text = response_payload["candidates"][0]["content"]["parts"][0]["text"]
        model_output = json.loads(extract_json_object(text))
        reasoning = GeminiReasoning.model_validate(model_output)
        return reasoning, True
    except Exception:
        return None, False


def evaluate_grounded_headline_use(
    reasoning: GeminiReasoning, input_headlines: list[str]
) -> bool:
    allowed = {headline.strip().casefold() for headline in input_headlines}
    if not allowed or not reasoning.evidence:
        return False
    for evidence in reasoning.evidence:
        if evidence.headline.strip().casefold() in allowed:
            return True
    return False


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
            movement_pct = price_change_pct(price, previous_price)

            conn.execute(
                """
                INSERT INTO price_snapshots (ticker, price, source, captured_at)
                VALUES (?, ?, ?, ?)
                """,
                (ticker, price, price_source, now),
            )

            inserted_news_items = 0
            newly_inserted_headlines: list[str] = []
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
                if result.rowcount:
                    newly_inserted_headlines.append(item.headline)

            llm_should_run, trigger_reason = should_run_llm(
                movement_pct=movement_pct,
                inserted_news_count=inserted_news_items,
                threshold_pct=self.settings.llm_price_change_threshold_pct,
            )
            llm_payload = {
                "ticker": ticker,
                "current_price": price,
                "previous_price": previous_price,
                "movement_delta": movement_delta,
                "movement_pct": movement_pct,
                "timestamp": now,
                "top_news": [
                    {
                        "headline": item.headline,
                        "source": item.source,
                        "published_at": item.published_at,
                    }
                    for item in news_items[: self.settings.llm_max_headlines]
                ],
                "trigger_reason": trigger_reason,
            }

            with self.tracer.observation(
                name="summarize",
                as_type="generation",
                model=self.settings.gemini_model if llm_should_run else "rule-based-summary-v1",
                input=llm_payload,
            ) as summarize_span:
                llm_result: GeminiReasoning | None = None
                valid_json = False
                if llm_should_run:
                    llm_result, valid_json = generate_gemini_reasoning(self.settings, llm_payload)

                if llm_result is not None:
                    summary = llm_result.summary
                    sentiment = llm_result.sentiment
                    hypothesis_text = llm_result.hypothesis
                    grounded_headline_used = evaluate_grounded_headline_use(
                        llm_result, [item.headline for item in news_items]
                    )
                else:
                    summary = build_summary(ticker, price, movement_delta, news_items)
                    sentiment = movement_to_sentiment(movement_delta)
                    hypothesis_text = build_hypothesis(ticker, movement_delta, news_items)
                    grounded_headline_used = len(news_items) > 0

                summarize_span.update(
                    output={
                        "summary": summary,
                        "sentiment": sentiment,
                        "movement_delta": movement_delta,
                        "movement_pct": movement_pct,
                        "llm_triggered": llm_should_run,
                        "valid_json": valid_json,
                    }
                )

            with self.tracer.observation(
                name="hypothesis",
                as_type="span",
                input={"ticker": ticker},
            ) as hypothesis_span:
                hypothesis_span.update(
                    output={"hypothesis": hypothesis_text},
                    metadata={
                        "grounded_headline_used": grounded_headline_used,
                        "valid_json": valid_json,
                        "llm_triggered": llm_should_run,
                        "trigger_reason": trigger_reason,
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
                    "movement_pct": movement_pct,
                    "llm_triggered": llm_should_run,
                    "valid_json": valid_json,
                    "trigger_reason": trigger_reason,
                    "newly_inserted_headlines": newly_inserted_headlines,
                    "llm_payload": llm_payload,
                    "llm_result": llm_result.model_dump() if llm_result else None,
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
                    "movement_pct": movement_pct,
                    "news_items": len(news_items),
                    "new_news_items": inserted_news_items,
                    "llm_triggered": llm_should_run,
                    "trigger_reason": trigger_reason,
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
