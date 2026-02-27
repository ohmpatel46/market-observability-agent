import sys
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.main import create_app
from app.settings import Settings


def make_client(tmp_path: Path) -> TestClient:
    settings = Settings(
        db_path=tmp_path / "test.db",
        alpha_vantage_api_key="demo",
        alpha_vantage_base_url="https://www.alphavantage.co/query",
        newsapi_api_key="mock_newsapi_key",
        newsapi_base_url="https://newsapi.org/v2/everything",
    )
    app = create_app(settings=settings)
    return TestClient(app)


def test_health_returns_ok(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_watchlist_seeded_and_crud(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        seeded = client.get("/watchlist")
        assert seeded.status_code == 200
        assert seeded.json()["tickers"] == ["AAPL", "MSFT", "TSLA"]

        add = client.post("/watchlist", json={"ticker": "nvda"})
        assert add.status_code == 201
        assert add.json() == {"ticker": "NVDA", "status": "added"}

        duplicate = client.post("/watchlist", json={"ticker": "NVDA"})
        assert duplicate.status_code == 409

        remove = client.delete("/watchlist/NVDA")
        assert remove.status_code == 200
        assert remove.json() == {"ticker": "NVDA", "status": "removed"}


def test_latest_and_history_pending_when_no_rows(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        latest = client.get("/latest/AAPL")
        assert latest.status_code == 200
        assert latest.json()["status"] == "pending"

        history = client.get("/history/AAPL")
        assert history.status_code == 200
        payload = history.json()
        assert payload["ticker"] == "AAPL"
        assert payload["count"] == 0
        assert payload["items"] == []


def test_external_sources_not_exposed(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        response = client.get("/config/external-sources")
        assert response.status_code == 404


def test_prices_and_news_pagination(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with make_client(tmp_path) as client:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO price_snapshots (ticker, price, source, captured_at)
                VALUES ('AAPL', 100.0, 'mock', '2026-01-01T00:00:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO price_snapshots (ticker, price, source, captured_at)
                VALUES ('AAPL', 101.5, 'mock', '2026-01-02T00:00:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO news_items (ticker, headline, url, source, published_at, fetched_at)
                VALUES ('AAPL', 'Headline 1', 'https://example.com/1', 'mock', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
                """
            )
            conn.execute(
                """
                INSERT INTO news_items (ticker, headline, url, source, published_at, fetched_at)
                VALUES ('AAPL', 'Headline 2', 'https://example.com/2', 'mock', '2026-01-02T00:00:00+00:00', '2026-01-02T00:00:00+00:00')
                """
            )
            conn.commit()

        prices_page_1 = client.get("/prices/AAPL?page=1&limit=1")
        assert prices_page_1.status_code == 200
        payload = prices_page_1.json()
        assert payload["total"] == 2
        assert payload["page"] == 1
        assert payload["limit"] == 1
        assert payload["has_next"] is True
        assert payload["items"][0]["price"] == 101.5

        prices_page_2 = client.get("/prices/AAPL?page=2&limit=1")
        assert prices_page_2.status_code == 200
        assert prices_page_2.json()["items"][0]["price"] == 100.0

        news_page_1 = client.get("/news/AAPL?page=1&limit=1")
        assert news_page_1.status_code == 200
        news_payload = news_page_1.json()
        assert news_payload["total"] == 2
        assert news_payload["has_next"] is True
        assert news_payload["items"][0]["headline"] == "Headline 2"

        news_page_2 = client.get("/news/AAPL?page=2&limit=1")
        assert news_page_2.status_code == 200
        assert news_page_2.json()["items"][0]["headline"] == "Headline 1"
