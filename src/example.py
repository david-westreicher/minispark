from pathlib import Path

from mini_spark.dataframe import DataFrame
from mini_spark.io import BlockFile
from mini_spark.sql import Col
from mini_spark.utils import TRACER

BlockFile(Path("fruit_color.bin")).write_rows(
    [
        {"id": "apple", "color": "red"},
        {"id": "banana", "color": "yellow"},
    ]
    * 1000,
)
BlockFile(Path("fruit_count.bin")).write_rows(
    [{"id": fruit, "count": i} for i in range(100) for fruit in ["apple", "banana"]]
)
colors = DataFrame().table("fruit_color.bin").select(Col("id").alias("color_id"), Col("color"))
colors.show()
counts = DataFrame().table("fruit_count.bin").select(Col("id").alias("count_id"), Col("count"))
counts.show()
colors.join(counts, on=Col("color_id") == Col("count_id"), how="inner").show(n=20)
TRACER.save("trace.pftrace")
