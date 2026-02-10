"""
View Mart Data - See the calculated inflation metrics
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
    f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
)

print("=" * 70)
print("MART DATA EXPLORATION")
print("=" * 70)

# 1. Latest inflation by state
print("\n1️⃣  Latest Inflation Rates by State (Dec 2025):")
latest_inflation = pd.read_sql("""
    SELECT 
        state,
        date,
        index_value as cpi,
        ROUND(mom_change::numeric, 2) as mom_pct,
        ROUND(yoy_change::numeric, 2) as yoy_pct
    FROM mart.inflation_by_state
    WHERE date = (SELECT MAX(date) FROM mart.inflation_by_state)
    ORDER BY yoy_change DESC NULLS LAST
""", engine)
print(latest_inflation)

# 2. Category inflation
print("\n2️⃣  Latest Inflation by Category:")
category_inflation = pd.read_sql("""
    SELECT 
        category_name,
        ROUND(avg_index::numeric, 2) as avg_cpi,
        ROUND(mom_change::numeric, 2) as mom_pct,
        ROUND(yoy_change::numeric, 2) as yoy_pct
    FROM mart.inflation_by_category
    WHERE date = (SELECT MAX(date) FROM mart.inflation_by_category)
    ORDER BY yoy_change DESC NULLS LAST
""", engine)
print(category_inflation)

# 3. State comparison
print("\n3️⃣  State Rankings (Most to Least Expensive):")
state_comp = pd.read_sql("""
    SELECT 
        rank_overall,
        state,
        ROUND(overall_cpi::numeric, 2) as cpi,
        ROUND(pct_vs_cheapest::numeric, 2) as pct_above_cheapest,
        region
    FROM mart.state_comparison
    ORDER BY rank_overall
""", engine)
print(state_comp)

# 4. Highest inflating states
print("\n4️⃣  Top 5 Highest Inflation States (YoY):")
top_inflation = pd.read_sql("""
    SELECT 
        state,
        ROUND(yoy_change::numeric, 2) as inflation_rate_pct
    FROM mart.inflation_by_state
    WHERE date = (SELECT MAX(date) FROM mart.inflation_by_state)
      AND yoy_change IS NOT NULL
    ORDER BY yoy_change DESC
    LIMIT 5
""", engine)
print(top_inflation)

print("\n" + "=" * 70)
print("✅ Mart data looks great!")
print("=" * 70)