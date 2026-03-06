import os
from pathlib import Path

from dotenv import load_dotenv


def load_environment(*, override: bool = False) -> None:
    """Load environment variables from a local .env file when present."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path, override=override)


def get_database_url() -> str:
    load_environment()
    # Canonical local configuration comes from POSTGRES_* variables.
    # DATABASE_URL is optional fallback/override for non-local environments.
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db_name = os.getenv("POSTGRES_DB")

    if user and password and host and port and db_name:
        return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    return "postgresql://postgres:postgres@localhost:5432/social_attribution_engine"
