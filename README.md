# ⚡ minispark ⚡
A **minimal Spark-like query engine** built for learning and experimentation.  
**minispark** supports both SQL and a DataFrame API, with multiple execution backends — from a pure Python interpreter to a compiled Zig engine.

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

## 🔬 Inner workings

This section explains what happens inside **minispark** when you run a query — from text to final result.

### 1) Start with some data and a query
#### Users
```bash
╭───────────┬──────────────┬─────────────┬───────┬───────────╮
│   user_id │ first_name   │ last_name   │   age │ country   │
├───────────┼──────────────┼─────────────┼───────┼───────────┤
│         1 │ Alice        │ Smith       │    25 │ USA       │
│         2 │ Bob          │ Johnson     │    30 │ Canada    │
│         3 │ Charlie      │ Brown       │    22 │ USA       │
│         4 │ David        │ Wilson      │    35 │ UK        │
│         5 │ Eva          │ Davis       │    28 │ Canada    │
│         6 │ Frank        │ Miller      │    40 │ USA       │
│         7 │ Grace        │ Taylor      │    27 │ UK        │
│         8 │ Hank         │ Anderson    │    32 │ USA       │
│         9 │ Ivy          │ Thomas      │    26 │ Canada    │
│        10 │ Jack         │ Jackson     │    24 │ USA       │
╰───────────┴──────────────┴─────────────┴───────┴───────────╯
```
#### Orders
```bash
╭────────────┬───────────┬───────────┬────────────┬─────────┬─────────────────────╮
│   order_id │   user_id │ product   │   quantity │   price │ order_date          │
├────────────┼───────────┼───────────┼────────────┼─────────┼─────────────────────┤
│          1 │         1 │ Laptop    │          1 │    1200 │ 2025-01-01 00:00:00 │
│          2 │         2 │ Mouse     │          2 │      25 │ 2025-01-05 00:00:00 │
│          3 │         3 │ Keyboard  │          1 │      45 │ 2025-02-10 00:00:00 │
│          4 │         1 │ Monitor   │          2 │     300 │ 2025-03-15 00:00:00 │
│          5 │         4 │ Laptop    │          1 │    1100 │ 2025-03-20 00:00:00 │
│          6 │         5 │ Mouse     │          1 │      30 │ 2025-04-01 00:00:00 │
│          7 │         6 │ Keyboard  │          2 │      50 │ 2025-04-10 00:00:00 │
│          8 │         7 │ Monitor   │          1 │     280 │ 2025-05-05 00:00:00 │
│          9 │         8 │ Laptop    │          1 │    1300 │ 2025-05-10 00:00:00 │
│         10 │         9 │ Mouse     │          3 │      27 │ 2025-06-01 00:00:00 │
╰────────────┴───────────┴───────────┴────────────┴─────────┴─────────────────────╯
```

#### Query:
```sql
SELECT u.country, COUNT() AS orders_count, SUM(o.quantity*o.price) AS total_sales
FROM 'users' AS u
    JOIN 'orders' AS o ON u.user_id=o.user_id
GROUP BY u.country
HAVING SUM(o.quantity*o.price) > 500;
```

### 2) Parsing (PEG — Parsimonious)

- The SQL text is passed to a **PEG parser** implemented with [Parsimonious](https://github.com/erikrose/parsimonious).  
- The result of this parsing step is a *DataFrame* object that represents the query in a structured way.

```python
df = (
    DataFrame()
    .table('users').alias('u')
    .join(
        DataFrame().table('orders').alias('o'),
        on=Col('u.user_id') == Col('o.user_id'),
        how="inner"
    )
    .group_by(Col('u.country'))
    .agg(
        F.count().alias('orders_count'),
        F.sum(Col('o.quantity') * Col('o.price')).alias('total_sales')
    )
    .filter(F.col("total_sales") > 500)
    .select(Col("u.country"), Col("orders_count"), Col("total_sales"))
)
```

Notice that the translation from SQL to the *DataFrame* is not straightforward:

- Selections are done in the end
- `HAVING` conditions are done after the aggregation
- `COUNT` and `SUM` appear in the select statement but need to be computed during aggregation

### 3) Logical Plan

This dataframe is now converted into a logical plan

```txt
 Project(u.country, orders_count, total_sales):None
  +-  Filter((_having_sum_o.quantity_mul_o.price) > (500)):None
    +-  AggregateTask(group_by: u.country, agg: [
            AggCol(original_col=Lit(value=1), name='orders_count', type='sum'),
            AggCol(original_col=BinaryOperatorColumn(left_side=o.quantity, right_side=o.price, operator=<built-in function mul>, left_type_convert_to=None, right_type_convert_to=None), name='total_sales', type='sum'),
            AggCol(original_col=BinaryOperatorColumn(left_side=o.quantity, right_side=o.price, operator=<built-in function mul>, left_type_convert_to=None, right_type_convert_to=None), name='_having_sum_o.quantity_mul_o.price', type='sum')
        ]):None
      +-  JoinTask((u.user_id) == (o.user_id), "inner"):None
        +-  LoadTableBlockTask(users):None
        +-  LoadTableBlockTask(orders):None
```

This format is similar to what you would find in other query engines like Spark or DuckDB.
You read it from the bottom up, indentations indicate data flow, the `:none` at the end will be explained soon.

- 2 `LoadTableBlock` tasks read the `users` and `orders` tables
- The `JoinTask` combines them into a new intermediate table
- The `AggregateTask` groups by country and computes the aggregations
  - Notice that there are 3 aggregations
  - The last one is an internal one used to compute the `HAVING` condition
  - Each aggregation stores the computation needed to compute it
    - `orders_count` is a `sum` over the literal `1` -> this computes the count
    - `total_sales` is a `sum` over the expression (`BinaryOperatorColumn`) `o.quantity * o.price`
- The `Filter` task applies the `HAVING` condition
- The `Project` task selects the final columns to return


## 📚 Why **minispark**?

**minispark** is a **toy project** designed to:
- Learn how query engines and Spark-like systems work internally  
- Explore query compilation/planning and execution strategies 

It’s **not production-ready**!

