from pathlib import Path

from mini_spark.dataframe import DataFrame
from mini_spark.execution import DistributedExecutionEngine
from mini_spark.io import BlockFile
from mini_spark.sql import Col

FRUIT_FILE = Path("big_fruit_count.bin")
if not FRUIT_FILE.exists():
    BlockFile(FRUIT_FILE).write_rows(
        [{"id": fruit, "count": i} for i in range(50_000_000) for fruit in ["apple", "banana"]]
    )

with DistributedExecutionEngine() as engine:
    counts = (
        DataFrame(engine)
        .table(str(FRUIT_FILE))
        .select(Col("id").alias("count_id"), ((Col("count") + 3 - 2) * 5).alias("count"))
        .filter((Col("count") % 1000) == 0)
    )
    counts.show()
