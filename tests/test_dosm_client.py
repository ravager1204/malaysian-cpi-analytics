from unittest.mock import patch

import pandas as pd

from data_ingestion.dosm_client import DOSMClient


def test_get_cpi_data_parses_dates():
    frame = pd.DataFrame(
        {
            "state": ["Malaysia"],
            "date": ["2025-12-01"],
            "division": ["overall"],
            "index": [132.5],
        }
    )

    with patch("pandas.read_parquet", return_value=frame.copy()) as mocked_read:
        result = DOSMClient().get_cpi_data()

    mocked_read.assert_called_once()
    assert pd.api.types.is_datetime64_any_dtype(result["date"])
    assert result.loc[0, "state"] == "Malaysia"


def test_get_categories_returns_dataframe():
    frame = pd.DataFrame({"division": ["01"], "desc_en": ["Food"], "digits": [2]})

    with patch("pandas.read_parquet", return_value=frame.copy()):
        result = DOSMClient().get_categories()

    assert list(result.columns) == ["division", "desc_en", "digits"]

