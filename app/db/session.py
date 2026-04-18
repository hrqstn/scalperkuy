from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from app.config import load_config


def get_engine() -> Engine:
    config = load_config()
    return create_engine(config.database_url, pool_pre_ping=True, future=True)
