"""PostgreSQL connections and schema migration entry point."""

from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from relay.config import Settings


def connect(dsn: str | None = None):
    return psycopg.connect(
        dsn if dsn is not None else Settings.from_env().database_url, row_factory=dict_row
    )


def make_pool(dsn: str, max_size: int = 10):
    return ConnectionPool(dsn, min_size=1, max_size=max_size, kwargs={"row_factory": dict_row})


def apply_migrations(dsn: str | None = None) -> None:
    """Serialize migration runners and commit SQL + version tracking atomically."""
    directory = Path(__file__).resolve().parents[1] / "migrations"
    with connect(dsn) as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (817392645,))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        applied = {
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for path in sorted(directory.glob("*.sql")):
            if path.name not in applied:
                conn.execute(path.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,))


if __name__ == "__main__":
    apply_migrations()
