from mini_spark.dataframe import DataFrame, Col
from pathlib import Path
from mini_spark.io import ColumnType, serialize

serialize(
    schema=[
        ("id", ColumnType.STRING),
        ("color", ColumnType.STRING),
    ],
    data=[
        ("apple", "red"),
        ("banana", "yellow"),
    ],
    output_file=Path("fruit_color.bin"),
)
serialize(
    schema=[
        ("id", ColumnType.STRING),
        ("count", ColumnType.INTEGER),
    ],
    data=[
        ("apple", 3),
        ("banana", 4),
    ],
    output_file=Path("fruit_count.bin"),
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
