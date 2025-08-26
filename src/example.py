from mini_spark.dataframe import DataFrame, Col
from pathlib import Path
from mini_spark.io import BlockFile

BlockFile(Path("fruit_color.bin")).write_rows(
    [
        {"id": "apple", "color": "red"},
        {"id": "banana", "color": "yellow"},
    ]
)
BlockFile(Path("fruit_count.bin")).write_rows(
    [
        {"id": "apple", "count": 3},
        {"id": "banana", "count": 4},
    ]
)
colors = (
    DataFrame()
    .table("fruit_color.bin")
    .select(Col("id").alias("color_id"), Col("color"))
)
# colors.show()
counts = (
    DataFrame()
    .table("fruit_count.bin")
    .select(Col("id").alias("count_id"), Col("count"))
)
# counts.show()
colors.join(counts, on=Col("color_id") == Col("count_id"), how="inner").show()
