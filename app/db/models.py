from __future__ import annotations

from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine


MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def init_db(engine: Engine) -> None:
    with engine.begin() as conn:
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            statements = [chunk.strip() for chunk in path.read_text(encoding="utf-8").split(";") if chunk.strip()]
            for statement in statements:
                conn.execute(text(statement))
