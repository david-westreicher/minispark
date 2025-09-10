import time
from pathlib import Path

from mini_spark.dataframe import DataFrame
from mini_spark.execution import ThreadEngine
from mini_spark.io import BlockFile
from mini_spark.sql import Col


def test_overflow(tmp_path: Path):
    test_file = tmp_path / "fruits.bin"
    test_data = [
        {"num1": 2**31 - 1, "num2": 2**31 - 1},
    ]
    BlockFile(test_file).write_rows(test_data)
    # act
    with ThreadEngine(tmp_path / "local") as engine:
        rows = DataFrame(engine).table(str(test_file)).select(Col("num1") + Col("num2")).collect()

    expected_rows = [
        {"num1_add_num2": -2},
    ]
    assert rows == expected_rows
    time.sleep(1)
