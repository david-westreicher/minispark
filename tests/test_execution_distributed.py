import time
from pathlib import Path

import pytest

from mini_spark.dataframe import DataFrame
from mini_spark.execution import LocalWorkerEngine
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


def test_groupby(test_data: str, tmp_path: Path):
    # act
    with LocalWorkerEngine(tmp_path / "local") as engine:
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
    time.sleep(1)


def test_overflow(tmp_path: Path):
    test_file = tmp_path / "fruits.bin"
    test_data = [
        {"num1": 2**31 - 1, "num2": 2**31 - 1},
    ]
    BlockFile(test_file).write_rows(test_data)
    # act
    with LocalWorkerEngine(tmp_path / "local") as engine:
        rows = DataFrame(engine).table(str(test_file)).select(Col("num1") + Col("num2")).collect()

    expected_rows = [
        {"num1_add_num2": -2},
    ]
    assert rows == expected_rows
    time.sleep(1)
