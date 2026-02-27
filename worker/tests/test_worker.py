import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from main import WorkerService, WorkerSettings, init_db, mock_price_for_ticker


def make_settings(tmp_path: Path) -> WorkerSettings:
    return WorkerSettings(
        db_path=tmp_path / "worker_test.db",
        interval_seconds=1,
        run_once=True,
        alpha_vantage_api_key="mock_alpha_vantage_key",
        alpha_vantage_base_url="https://www.alphavantage.co/query",
        newsapi_api_key="mock_newsapi_key",
        newsapi_base_url="https://newsapi.org/v2/everything",
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
