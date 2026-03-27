"""
Quick Data Analysis & Visualization
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine

from config.settings import get_settings

sns.set_style("whitegrid")
settings = get_settings()
settings.ensure_runtime_dirs()
engine = create_engine(settings.database.sqlalchemy_url)

print("=" * 70)
print("QUICK CPI ANALYSIS")
print("=" * 70)

print("\nGenerating national inflation trend...")
national = pd.read_sql(
    """
    SELECT date, index
    FROM raw.cpi_data
    WHERE state = 'Malaysia' AND division = 'overall'
    ORDER BY date
    """,
    engine,
)

plt.figure(figsize=(14, 6))
plt.plot(national["date"], national["index"], linewidth=2, color="#2E86AB")
plt.title("Malaysia Overall CPI Trend (2010-2025)", fontsize=16, fontweight="bold")
plt.xlabel("Date", fontsize=12)
plt.ylabel("CPI Index (2010=100)", fontsize=12)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(settings.outputs_dir / "national_cpi_trend.png", dpi=150)
print("Saved: data/outputs/national_cpi_trend.png")
plt.close()

print("\nGenerating state comparison...")
latest_states = pd.read_sql(
    """
    SELECT state, index
    FROM raw.cpi_data
    WHERE division = 'overall'
      AND date = (SELECT MAX(date) FROM raw.cpi_data)
      AND state != 'Malaysia'
    ORDER BY index DESC
    """,
    engine,
)

plt.figure(figsize=(10, 8))
colors = ["#E63946" if x > 135 else "#06A77D" for x in latest_states["index"]]
plt.barh(latest_states["state"], latest_states["index"], color=colors)
plt.axvline(
    x=latest_states["index"].mean(),
    color="black",
    linestyle="--",
    label=f"Average: {latest_states['index'].mean():.1f}",
    linewidth=2,
)
plt.title("CPI by State (December 2025)", fontsize=16, fontweight="bold")
plt.xlabel("CPI Index", fontsize=12)
plt.ylabel("State", fontsize=12)
plt.legend()
plt.tight_layout()
plt.savefig(settings.outputs_dir / "state_comparison.png", dpi=150)
print("Saved: data/outputs/state_comparison.png")
plt.close()

print("\nQuick Statistics:")
if len(national) > 0:
    print(f"   National CPI (Dec 2025): {national['index'].iloc[-1]:.1f}")
else:
    print("   National CPI: Not available in dataset")

highest_state = latest_states.iloc[0]
lowest_state = latest_states.iloc[-1]
price_gap = highest_state["index"] - lowest_state["index"]
pct_gap = ((highest_state["index"] / lowest_state["index"]) - 1) * 100

print(f"   Highest State: {highest_state['state']} ({highest_state['index']:.1f})")
print(f"   Lowest State: {lowest_state['state']} ({lowest_state['index']:.1f})")
print(f"   Price Gap: {price_gap:.1f} points")
print(
    f"   That's {pct_gap:.1f}% difference!"
)
