"""
Quick Data Analysis & Visualization
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
sns.set_style('whitegrid')

# Connect
engine = create_engine(
    f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
    f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
)

print("=" * 70)
print("QUICK CPI ANALYSIS")
print("=" * 70)

# 1. National inflation trend
print("\n📈 Generating national inflation trend...")
national = pd.read_sql("""
    SELECT date, index
    FROM raw.cpi_data
    WHERE state = 'Malaysia' AND division = 'overall'
    ORDER BY date
""", engine)

plt.figure(figsize=(14, 6))
plt.plot(national['date'], national['index'], linewidth=2, color='#2E86AB')
plt.title('Malaysia Overall CPI Trend (2010-2025)', fontsize=16, fontweight='bold')
plt.xlabel('Date', fontsize=12)
plt.ylabel('CPI Index (2010=100)', fontsize=12)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('data/outputs/national_cpi_trend.png', dpi=150)
print("✅ Saved: data/outputs/national_cpi_trend.png")
plt.close()

# 2. Latest state comparison
print("\n🗺️  Generating state comparison...")
latest_states = pd.read_sql("""
    SELECT state, index
    FROM raw.cpi_data
    WHERE division = 'overall'
      AND date = (SELECT MAX(date) FROM raw.cpi_data)
      AND state != 'Malaysia'
    ORDER BY index DESC
""", engine)

plt.figure(figsize=(10, 8))
colors = ['#E63946' if x > 135 else '#06A77D' for x in latest_states['index']]
plt.barh(latest_states['state'], latest_states['index'], color=colors)
plt.axvline(x=latest_states['index'].mean(), color='black', linestyle='--', 
            label=f'Average: {latest_states["index"].mean():.1f}', linewidth=2)
plt.title('CPI by State (December 2025)', fontsize=16, fontweight='bold')
plt.xlabel('CPI Index', fontsize=12)
plt.ylabel('State', fontsize=12)
plt.legend()
plt.tight_layout()
plt.savefig('data/outputs/state_comparison.png', dpi=150)
print("✅ Saved: data/outputs/state_comparison.png")
plt.close()

# 3. Calculate simple stats
print("\n📊 Quick Statistics:")

# Check if we have national data
if len(national) > 0:
    print(f"   National CPI (Dec 2025): {national['index'].iloc[-1]:.1f}")
else:
    print(f"   National CPI: Not available in dataset")

print(f"   Highest State: {latest_states['state'].iloc[0]} ({latest_states['index'].iloc[0]:.1f})")
print(f"   Lowest State: {latest_states['state'].iloc[-1]} ({latest_states['index'].iloc[-1]:.1f})")
print(f"   Price Gap: {latest_states['index'].iloc[0] - latest_states['index'].iloc[-1]:.1f} points")
print(f"   That's {((latest_states['index'].iloc[0] / latest_states['index'].iloc[-1]) - 1) * 100:.1f}% difference!")