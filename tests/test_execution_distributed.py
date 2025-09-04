from pathlib import Path

import pytest

from mini_spark.dataframe import DataFrame
from mini_spark.execution import DistributedExecutionEngine
from mini_spark.io import BlockFile
from mini_spark.sql import Col


@pytest.fixture
def test_data(tmp_path: Path) -> str:
    test_file = tmp_path / "fruits.bin"
    test_data = [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]
    BlockFile(test_file).write_rows(test_data)
    return str(test_file)


def test_groupby(test_data: str):
    # act
    with DistributedExecutionEngine() as engine:
        rows = DataFrame(engine).table(test_data).group_by(Col("fruit")).count().collect()

    # assert
    # TODO(david): implement assertDataFrameEquals
    expected_rows = [
        {"fruit": "apple", "count": 2},
        {"fruit": "banana", "count": 2},
        {"fruit": "orange", "count": 1},
    ]
    sort_key = lambda row: row["fruit"]  # noqa: E731
    rows.sort(key=sort_key)
    expected_rows.sort(key=sort_key)
    assert rows == expected_rows
