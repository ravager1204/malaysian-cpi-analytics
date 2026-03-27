import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import create_engine, text

from config.settings import get_settings

settings = get_settings()
engine = create_engine(settings.database.sqlalchemy_url)

print("Creating staging tables...")

with open(
    settings.project_root / "sql" / "ddl" / "staging_tables.sql",
    encoding="utf-8",
) as file_handle:
    sql = file_handle.read()

with engine.connect() as conn:
    conn.execute(text(sql))
    conn.commit()

print("Staging tables created")

tables = pd.read_sql(
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'staging'
    ORDER BY table_name
    """,
    engine,
)
print("\nStaging tables:")
print(tables)
