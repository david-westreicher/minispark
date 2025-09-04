from pathlib import Path

import pytest

from mini_spark.codegen import compile_plan
from mini_spark.dataframe import DataFrame
from mini_spark.io import BlockFile
from mini_spark.plan import PhysicalPlan
from mini_spark.sql import Col


@pytest.fixture
def test_data(tmp_path: Path) -> str:
    test_file = tmp_path / "fruits.bin"
    test_data = [
        {"fruit": "apple", "quantity": 3, "color": "red"},
    ]
    BlockFile(test_file).write_rows(test_data)
    return str(test_file)


def test_codegen(test_data: str):
    task = DataFrame().table(test_data).filter(Col("color") == "red").select(Col("quantity") + 2, Col("fruit")).task
    plan = PhysicalPlan.generate_physical_plan(task)
    compile_plan(plan)
