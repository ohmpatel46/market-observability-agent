from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path
    alpha_vantage_api_key: str
    alpha_vantage_base_url: str
    newsapi_api_key: str
    newsapi_base_url: str


def load_settings() -> Settings:
    import os

    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    db_path = Path(os.getenv("DB_PATH", str(data_dir / "market_observability.db")))

    # Docs:
    # Alpha Vantage: https://www.alphavantage.co/documentation/
    # NewsAPI: https://newsapi.org/docs/endpoints/everything
    return Settings(
        db_path=db_path,
        alpha_vantage_api_key=os.getenv("ALPHA_VANTAGE_API_KEY", "demo"),
        alpha_vantage_base_url=os.getenv(
            "ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"
        ),
        newsapi_api_key=os.getenv("NEWSAPI_API_KEY", "mock_newsapi_key"),
        newsapi_base_url=os.getenv(
            "NEWSAPI_BASE_URL", "https://newsapi.org/v2/everything"
        ),
    )
