from pathlib import Path

from mini_spark.constants import Row
from mini_spark.dataframe import DataFrame
from mini_spark.execution import PythonExecutionEngine
from mini_spark.io import BlockFile
from mini_spark.sql import Col
from mini_spark.sql import Functions as F  # noqa: N817

test_table = Path("some_database_file.bin")
test_data: list[Row] = [
    {"fruit": "apple", "quantity": 3, "color": "red", "price": 1.5},
    {"fruit": "banana", "quantity": 5, "color": "yellow", "price": 1.9},
    {"fruit": "orange", "quantity": 2, "color": "orange", "price": 1.2},
    {"fruit": "orange", "quantity": 4, "color": "orange", "price": 2.2},
]
BlockFile(Path(test_table)).write_rows(test_data)


with PythonExecutionEngine() as engine:
    rows = (
        DataFrame(engine)
        .table(str(test_table))
        .group_by(Col("fruit"))
        .agg(F.sum(Col("quantity") * Col("price")).alias("total_price"))
        .show()
    )
