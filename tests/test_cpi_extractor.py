import shutil
from pathlib import Path

import pandas as pd
import pytest

from data_ingestion.cpi_extractor import CPIExtractor


class StubClient:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_cpi_data(self, granularity: str = "2d") -> pd.DataFrame:
        return self.df.copy()


def test_extract_full_saves_parquet():
    df = pd.DataFrame(
        {
            "state": ["Malaysia"],
            "date": pd.to_datetime(["2025-12-01"]),
            "division": ["overall"],
            "index": [132.5],
        }
    )
    extractor = CPIExtractor(StubClient(df))
    artifact_dir = Path("tests") / "_artifacts" / "extractor"
    if artifact_dir.exists():
        shutil.rmtree(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    save_path = artifact_dir / "cpi_latest.parquet"

    result = extractor.extract_full(save_path=save_path)

    assert len(result) == 1
    assert save_path.exists()

    shutil.rmtree(artifact_dir)


def test_validate_data_raises_on_missing_columns():
    invalid_df = pd.DataFrame({"state": ["Malaysia"], "date": ["2025-12-01"]})
    extractor = CPIExtractor(StubClient(invalid_df))

    with pytest.raises(ValueError, match="Missing columns"):
        extractor.extract_full()
