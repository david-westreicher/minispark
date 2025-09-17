# ⚡ minispark ⚡
A **minimal Spark-like query engine** built for learning and experimentation.  
**minispark** supports both SQL and a DataFrame API, with multiple execution backends — from a pure Python engine to a compiled native execution engine (that should be super fast 🚀, powered by [Zig](https://ziglang.org)).

Checkout the [Inner Workings](docs/InnerWorkings.md) section to see how it works under the hood!

![Shell Demo](docs/shell.gif)


## ✨ Features  

- **SQL support**:  
  - `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `JOIN`  
- **Data types**: `INT`, `FLOAT`, `STRING`, `TIMESTAMP`  
- **Functions and Expressions**: 
  - Aggregation functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
  - Arithmetic functions (`+`,`-`,`*`,`/`,...)
  - String functions (`CONCAT`, `LIKE`)
  - Timestamp functions (`BETWEEN`)
- **DataFrame API**: Similar to PySpark  
- **Execution engines**:  
  - 🐍 **PythonEngine**: reference implementation (interpreted, slower)  
  - ⚡ **ThreadPoolEngine**: compiles queries to Zig and executes natively  
- **Shuffle stages** for distributed-style `GROUP BY` and `JOIN`  
- **Tracing** to debug performance issues (Visualization with [perfetto](https://perfetto.dev/))

## 📦 Dependencies  

- **Python** 3.13  
- **Zig** 0.15.1  

## 🔧 Installation  

Clone the repo and install locally (use a virtual environment if desired).
```bash
git clone https://github.com/david-westreicher/minispark.git
cd minispark
pip install .
```

## 🧪 Running Tests  

Run the test suite to ensure **minispark** is working correctly.  
```bash
pytest
```

## 💻 Interactive Shell  
```bash
python -m mini_spark.shell
```

**minispark** comes with a lightweight interactive shell.  
- Keeps **command history**  
- Lets you **execute SQL queries** and see results immediately  

Example session output shows query results in a simple table format.  

## 🔍 Example Usage  

### Using the DataFrame API  

**minispark** supports [DataFrame](https://en.wikipedia.org/wiki/Apache_Spark#Spark_SQL) operations like filtering, grouping, counting, and applying conditions, similar to [PySpark](https://spark.apache.org/docs/latest/api/python/index.html). You can chain multiple transformations and display/collect the results. Check [the examples folder](examples/) for more example queries and scripts.

```python
from pathlib import Path

from mini_spark.constants import Row
from mini_spark.dataframe import DataFrame
from mini_spark.execution import PythonExecutionEngine
from mini_spark.io import BlockFile
from mini_spark.sql import Col
from mini_spark.sql import Functions as F  # noqa: N817

# create a test table
test_table = Path("some_database_file.bin")
test_data: list[Row] = [
    {"fruit": "apple", "quantity": 3, "color": "red", "price": 1.5},
    {"fruit": "banana", "quantity": 5, "color": "yellow", "price": 1.9},
    {"fruit": "orange", "quantity": 2, "color": "orange", "price": 1.2},
    {"fruit": "orange", "quantity": 4, "color": "orange", "price": 2.2},
]
BlockFile(Path(test_table)).write_rows(test_data)


# calculate total price per fruit
with PythonExecutionEngine() as engine:
    rows = (
        DataFrame(engine)
        .table(str(test_table))
        .group_by(Col("fruit"))
        .agg(F.sum(Col("quantity") * Col("price")).alias("total_price"))
        .show()
    )
```

**Output**
```bash
╭─────────┬───────────────╮
│ fruit   │   total_price │
├─────────┼───────────────┤
│ apple   │           4.5 │
│ banana  │           9.5 │
│ orange  │          11.2 │
╰─────────┴───────────────╯
```


### Using SQL

You can create a session, load data, register it as a temporary view, and run SQL queries with filters, aggregations, and groupings. The results can be displayed directly.

```python
from mini_spark.dataframe import DataFrame
from mini_spark.execution import PythonExecutionEngine, ThreadEngine

query = """
SELECT
    fruit,
    SUM(quantity * price) AS total_price
FROM
    'some_database_file.bin'
GROUP BY
    fruit;
"""

with PythonExecutionEngine() as engine:
    rows = engine.sql(query).collect()
```

## ⚡ Execution Engines  

- **PythonEngine** (default) executes queries directly in Python and is easier to debug, though slower.  
- **ThreadEngine** compiles queries to Zig code and runs them across multiple threads for faster execution.  

You can switch between engines by configuration when creating a session.  

## 🏃 Benchmark
To test the implementation we use the [TCP-H](https://www.tpc.org/tpch/) benchmark. We run the benchmark with 4 worker threads. Here are the execution times for different scaling factors and the following query

```sql
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
     lineitem
WHERE
    l_shipdate <= '1998-12-01'
GROUP BY
    l_returnflag;
```


| `lineitem` sf | CSV file size | Rows       | Average Execution Time|
| ----------- | ------------: | ---------: | --------------------: |
| 1           | 738 MB        |  6.001.215 |                0.707s |
| 10          | 7.4 GB        | 59.986.052 |                3.372s |
| 15          | 12.0 GB       | 89.987.373 |                4.874s |


## 📚 Why **minispark**?

**minispark** is a **toy project** designed to:
- Learn how query engines and Spark-like systems work internally  
- Explore query compilation/planning and execution strategies 

It’s **not production-ready**!

