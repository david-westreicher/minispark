from pathlib import Path

from mini_spark.dataframe import DataFrame
from mini_spark.execution import ThreadEngine
from mini_spark.io import BlockFile
from mini_spark.sql import Col
from mini_spark.utils import TRACER

FRUIT_FILE = Path("big_fruit_count.bin")
if not FRUIT_FILE.exists():
    BlockFile(FRUIT_FILE).write_rows(
        [{"id": fruit, "count": i} for i in range(50_000_000) for fruit in ["apple", "banana"]]
    )

with ThreadEngine() as engine:
    # without engine: 98 secs
    # with engine: 1 secs
    counts = (
        DataFrame(engine)
        .table(str(FRUIT_FILE))
        .select(Col("id"), ((Col("count") + 3 - 2) * 5).alias("count"))
        .filter(((Col("count") % 10_000_000) == 0) & (Col("id") == "apple"))
    )
    counts.show(n=100)
    TRACER.save("trace.pftrace")
