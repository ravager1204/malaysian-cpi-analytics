"""
Data Exploration Script
Quick peek at what we have in the database
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import create_engine

from config.settings import get_settings

settings = get_settings()
engine = create_engine(settings.database.sqlalchemy_url)

print("=" * 70)
print("DATABASE EXPLORATION")
print("=" * 70)

print("\n1. Tables in raw schema:")
tables = pd.read_sql(
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'raw'
    """,
    engine,
)
print(tables)

print("\n2. CPI Data Summary:")
summary = pd.read_sql(
    """
    SELECT
        COUNT(*) as total_records,
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(DISTINCT state) as num_states,
        COUNT(DISTINCT division) as num_divisions
    FROM raw.cpi_data
    """,
    engine,
)
print(summary)

print("\n3. Latest Month CPI (Overall) by State:")
latest = pd.read_sql(
    """
    SELECT
        state,
        date,
        index as cpi_index
    FROM raw.cpi_data
    WHERE division = 'overall'
      AND date = (SELECT MAX(date) FROM raw.cpi_data)
    ORDER BY index DESC
    """,
    engine,
)
print(latest)

print("\n4. CPI Categories:")
categories = pd.read_sql(
    """
    SELECT DISTINCT division
    FROM raw.cpi_data
    WHERE division != 'overall'
    ORDER BY division
    """,
    engine,
)
print(categories)

print("\n5. Sample Category Definitions:")
cat_lookup = pd.read_sql(
    """
    SELECT division, desc_en
    FROM raw.categories
    WHERE digits = 2
    ORDER BY division
    LIMIT 15
    """,
    engine,
)
print(cat_lookup)

print("\n6. Recent Load History:")
history = pd.read_sql(
    """
    SELECT
        table_name,
        load_timestamp,
        records_loaded,
        load_status
    FROM raw.load_metadata
    ORDER BY load_timestamp DESC
    LIMIT 5
    """,
    engine,
)
print(history)

print("\n" + "=" * 70)
print("Exploration complete")
print("=" * 70)
