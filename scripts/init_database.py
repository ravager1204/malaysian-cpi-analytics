"""
Initialize the warehouse schemas and tables for a local end-to-end run.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from config.settings import get_settings


def execute_sql_file(engine, path: Path) -> None:
    with open(path, encoding="utf-8") as file_handle:
        sql = file_handle.read()

    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()


def main() -> int:
    settings = get_settings()
    engine = create_engine(settings.database.sqlalchemy_url)

    print("Initializing raw, staging, and mart schemas...")
    execute_sql_file(engine, settings.project_root / "sql" / "schema" / "01_create_schemas.sql")
    execute_sql_file(engine, settings.project_root / "sql" / "ddl" / "staging_tables.sql")
    execute_sql_file(engine, settings.project_root / "sql" / "ddl" / "mart_tables.sql")
    print("Database initialization complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
