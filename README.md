# ⚡ minispark ⚡
A **minimal Spark-like query engine** built for learning and experimentation.  
MiniSpark supports both SQL and a DataFrame API, with multiple execution backends — from a pure Python interpreter to a compiled Zig engine.  

---

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

---

## 📦 Dependencies  

- **Python** 3.13  
- **Zig** 0.15.1  

---

## 🔧 Installation  

Clone the repo and install locally:  
```bash
git clone https://github.com/david-westreicher/minispark.git
cd minispark
pip install .
```

## 🧪 Running Tests  

Run the test suite to ensure MiniSpark is working correctly.  
```bash
pytest
```

---

## 💻 Interactive Shell  
```bash
python -m mini_spark.shell
```

MiniSpark comes with a lightweight interactive shell.  
- Keeps **command history**  
- Lets you **execute SQL queries** and see results immediately  

Example session output shows query results in a simple table format.  

---

## 🔍 Example Usage  

### Using SQL  

You can create a session, load data, register it as a temporary view, and run SQL queries with filters, aggregations, and groupings. The results can be displayed directly.  

```python
from mini_spark.dataframe import DataFrame
from mini_spark.execution import PythonExecutionEngine, ThreadEngine

query = """
SELECT
    left_table.fruit AS fruit_left,
    left_table.color,
    right_table.quantity
FROM
    some_database_file_bin AS left_table
INNER JOIN
    test_data AS right_table
ON
    left_table.fruit = right_table.fruit;
"""

with PythonExecutionEngine() as engine:
    rows = engine.sql(query).collect()
```

### Using the DataFrame API  

MiniSpark supports [DataFrame](https://en.wikipedia.org/wiki/Apache_Spark#Spark_SQL) operations like filtering, grouping, counting, and applying conditions, similar to PySpark. You can chain multiple transformations and display the results.  

```python
from mini_spark.dataframe import DataFrame
from mini_spark.execution import PythonExecutionEngine, ThreadEngine
from mini_spark.sql import Col

with PythonExecutionEngine() as engine:
    rows = (
        DataFrame(engine)
        .table("some_database_file.bin")
        .select(Col("fruit").alias("fruit_left"), Col("color"))
        .join(
            DataFrame().table(test_data).select(Col("fruit").alias("fruit_right"), Col("quantity")),
            on=Col("fruit_left") == Col("fruit_right"),
            how="inner",
        )
        .collect()
    )
```

---

## ⚡ Execution Engines  

- **PythonEngine** (default) executes queries directly in Python and is easier to debug, though slower.  
- **ThreadEngine** compiles queries to Zig code and runs them across multiple threads for faster execution.  

You can switch between engines by configuration when creating a session.  

---

## 📚 Why MiniSpark?  

MiniSpark is a **toy project** designed to:  
- Learn how query engines and Spark-like systems work internally  
- Explore query compilation/planning and execution strategies 

It’s **not production-ready**!

