import sys
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
