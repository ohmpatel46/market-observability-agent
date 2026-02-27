import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import main
from main import (
    LangfuseTracer,
    WorkerService,
    WorkerSettings,
    init_db,
    mock_price_for_ticker,
    should_run_llm,
)


def make_settings(tmp_path: Path) -> WorkerSettings:
    return WorkerSettings(
        db_path=tmp_path / "worker_test.db",
        interval_seconds=1,
        run_once=True,
        alpha_vantage_api_key="mock_alpha_vantage_key",
        alpha_vantage_base_url="https://www.alphavantage.co/query",
        newsapi_api_key="mock_newsapi_key",
        newsapi_base_url="https://newsapi.org/v2/everything",
        gemini_api_key="",
        gemini_model="gemini-1.5-flash",
        llm_price_change_threshold_pct=0.5,
        llm_max_headlines=5,
        langfuse_public_key="",
        langfuse_secret_key="",
        langfuse_base_url="https://cloud.langfuse.com",
    )


def seed_watchlist(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO watchlist (ticker, created_at) VALUES (?, ?)",
            ("AAPL", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()


def test_run_cycle_writes_snapshots_news_and_analysis(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    init_db(settings.db_path)
    seed_watchlist(settings.db_path)

    service = WorkerService(settings)
    result = service.run_cycle()

    assert result["tickers_processed"] == 1
    assert result["snapshots_written"] == 1
    assert result["news_written"] >= 1
    assert result["analyses_written"] == 1

    with sqlite3.connect(settings.db_path) as conn:
        snapshot_count = conn.execute("SELECT COUNT(*) FROM price_snapshots").fetchone()[0]
        news_count = conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        analyses_count = conn.execute("SELECT COUNT(*) FROM analyses").fetchone()[0]
        analysis = conn.execute(
            "SELECT summary, sentiment FROM analyses WHERE ticker = 'AAPL' LIMIT 1"
        ).fetchone()

    assert snapshot_count == 1
    assert news_count >= 1
    assert analyses_count == 1
    assert "AAPL last price" in analysis[0]
    assert analysis[1] in {"neutral", "positive", "negative"}


def test_second_cycle_computes_delta(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    init_db(settings.db_path)
    seed_watchlist(settings.db_path)
    service = WorkerService(settings)

    service.run_cycle()
    service.run_cycle()

    with sqlite3.connect(settings.db_path) as conn:
        last_two = conn.execute(
            """
            SELECT movement_delta, raw_json
            FROM analyses
            WHERE ticker = 'AAPL'
            ORDER BY id DESC
            LIMIT 2
            """
        ).fetchall()

    assert len(last_two) == 2
    # Mock prices are deterministic per ticker, so second delta is exactly 0.
    assert float(last_two[0][0]) == 0.0
    assert str(mock_price_for_ticker("AAPL")) in last_two[0][1]


def test_langfuse_is_disabled_for_missing_credentials(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    tracer = LangfuseTracer(settings)
    assert tracer.enabled is False


def test_news_dedup_prevents_duplicate_rows(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    init_db(settings.db_path)
    seed_watchlist(settings.db_path)
    service = WorkerService(settings)

    def duplicated_news(*_: object, **__: object):
        items = main.mock_news_for_ticker("AAPL")
        return [items[0], items[0], items[1]], "mock_newsapi"

    original = main.fetch_news_items
    main.fetch_news_items = duplicated_news
    try:
        result = service.run_cycle()
    finally:
        main.fetch_news_items = original

    with sqlite3.connect(settings.db_path) as conn:
        news_count = conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]

    assert result["news_written"] == 2
    assert news_count == 2


def test_should_run_llm_uses_point_five_percent_threshold() -> None:
    run_for_price, reason_price = should_run_llm(
        movement_pct=0.6, inserted_news_count=0, threshold_pct=0.5
    )
    run_for_news, reason_news = should_run_llm(
        movement_pct=0.2, inserted_news_count=1, threshold_pct=0.5
    )
    skip, reason_skip = should_run_llm(
        movement_pct=0.2, inserted_news_count=0, threshold_pct=0.5
    )

    assert run_for_price is True
    assert reason_price == "price_change"
    assert run_for_news is True
    assert reason_news == "news_update"
    assert skip is False
    assert reason_skip == "none"
