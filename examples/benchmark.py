from pathlib import Path

from mini_spark.constants import ColumnType
from mini_spark.execution import ThreadEngine
from mini_spark.utils import TRACER, convert_csv_to_block_file

"""
Uses the TPC-H lineitem dataset to benchmark.
Generate the benchmark with duckdb:
    INSTALL tpch;
    LOAD tpch;
    CALL dbgen(sf = 15);
    COPY lineitem TO 'tmp/lineitem_sf15.csv' (FORMAT CSV, HEADER TRUE);

lineitem_sf15: 89.987.373 rows, 12.0 GB (csv), 8.9 GB (bin)
lineitem_sf10: 59.986.052 rows,  7.4 GB (csv), 6.0 GB (bin)
"""

STR = ColumnType.STRING
INT = ColumnType.INTEGER
FLOAT = ColumnType.FLOAT
DATE = ColumnType.TIMESTAMP

schema = [
    ("l_orderkey", INT),
    ("l_partkey", INT),
    ("l_suppkey", INT),
    ("l_linenumber", INT),
    ("l_quantity", FLOAT),
    ("l_extendedprice", FLOAT),
    ("l_discount", FLOAT),
    ("l_tax", FLOAT),
    ("l_returnflag", STR),
    ("l_linestatus", STR),
    ("l_shipdate", DATE),
    ("l_commitdate", DATE),
    ("l_receiptdate", DATE),
    ("l_shipinstruct", STR),
    ("l_shipmode", STR),
    ("l_comment", STR),
]

output_path = Path("tmp/lineitem_sf1.bin")
if not output_path.exists():
    convert_csv_to_block_file(
        csv_file=Path("tmp/lineitem_sf10.csv"),
        block_file=output_path,
        schema=schema,
    )

query = f"""
SELECT
    l_returnflag,
    SUM(l_quantity)        AS sum_qty,
    SUM(l_extendedprice)   AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount))              AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity)        AS avg_qty,
    AVG(l_extendedprice)   AS avg_price,
    AVG(l_discount)        AS avg_disc,
    COUNT()               AS count_order
FROM
     '{output_path}'
WHERE
    l_shipdate <= '1998-12-01'
GROUP BY
    l_returnflag;
"""  # noqa: S608

with ThreadEngine() as engine:
    engine.sql(query).show()
    TRACER.save("trace.pftrace")
