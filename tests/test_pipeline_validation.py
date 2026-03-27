from unittest.mock import patch

import pandas as pd

from scripts import validate_pipeline


def test_validate_pipeline_reads_expected_checks(capsys):
    responses = iter(
        [
            pd.DataFrame({"row_count": [10]}),
            pd.DataFrame({"row_count": [2]}),
            pd.DataFrame({"row_count": [10]}),
            pd.DataFrame({"row_count": [5]}),
            pd.DataFrame({"row_count": [4]}),
            pd.DataFrame({"row_count": [3]}),
            pd.DataFrame({"latest_date": [pd.Timestamp("2025-12-01")]}),
        ]
    )

    with patch("scripts.validate_pipeline.create_engine"), patch(
        "scripts.validate_pipeline.pd.read_sql",
        side_effect=lambda *args, **kwargs: next(responses),
    ):
        exit_code = validate_pipeline.main()

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "raw.cpi_data: 10 rows" in output
    assert "Latest mart date: 2025-12-01 00:00:00" in output
