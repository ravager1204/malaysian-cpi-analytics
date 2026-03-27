from unittest.mock import MagicMock, patch

import pandas as pd

from data_ingestion.db_loader import PostgreSQLLoader


def test_load_to_raw_uses_dataframe_to_sql():
    fake_engine = MagicMock()
    sample_df = pd.DataFrame({"state": ["Malaysia"]})

    with patch("data_ingestion.db_loader.create_engine", return_value=fake_engine):
        loader = PostgreSQLLoader()

    with patch.object(sample_df, "to_sql") as mocked_to_sql, patch.object(
        loader,
        "_log_load",
    ) as mocked_log:
        row_count = loader.load_to_raw(sample_df, "cpi_data")

    assert row_count == 1
    mocked_to_sql.assert_called_once()
    mocked_log.assert_called_once_with("cpi_data", 1, "SUCCESS")
