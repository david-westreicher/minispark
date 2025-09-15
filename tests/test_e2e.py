from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from mini_spark.constants import ColumnType, Row
from mini_spark.execution import ExecutionEngine, PythonExecutionEngine
from mini_spark.io import BlockFile
from mini_spark.parser import parse_sql
from mini_spark.plan import PhysicalPlan

pytest.skip("Skipping this test file", allow_module_level=True)

ENGINES = [PythonExecutionEngine]
FLOAT = ColumnType.FLOAT
INT = ColumnType.INTEGER
STR = ColumnType.STRING

USERS = [
    (1, "Alice", "Smith", 25, "USA"),
    (2, "Bob", "Johnson", 30, "Canada"),
    (3, "Charlie", "Brown", 22, "USA"),
    (4, "David", "Wilson", 35, "UK"),
    (5, "Eva", "Davis", 28, "Canada"),
    (6, "Frank", "Miller", 40, "USA"),
    (7, "Grace", "Taylor", 27, "UK"),
    (8, "Hank", "Anderson", 32, "USA"),
    (9, "Ivy", "Thomas", 26, "Canada"),
    (10, "Jack", "Jackson", 24, "USA"),
    (11, "Kate", "White", 29, "UK"),
    (12, "Leo", "Harris", 33, "USA"),
    (13, "Mia", "Martin", 31, "Canada"),
    (14, "Nick", "Thompson", 23, "UK"),
    (15, "Olivia", "Garcia", 36, "USA"),
]

ORDERS = [
    (1, 1, "Laptop", 1, 1200.0, "2025-01-01"),
    (2, 2, "Mouse", 2, 25.0, "2025-01-05"),
    (3, 3, "Keyboard", 1, 45.0, "2025-02-10"),
    (4, 1, "Monitor", 2, 300.0, "2025-03-15"),
    (5, 4, "Laptop", 1, 1100.0, "2025-03-20"),
    (6, 5, "Mouse", 1, 30.0, "2025-04-01"),
    (7, 6, "Keyboard", 2, 50.0, "2025-04-10"),
    (8, 7, "Monitor", 1, 280.0, "2025-05-05"),
    (9, 8, "Laptop", 1, 1300.0, "2025-05-10"),
    (10, 9, "Mouse", 3, 27.0, "2025-06-01"),
    (11, 10, "Keyboard", 1, 40.0, "2025-06-15"),
    (12, 11, "Monitor", 2, 290.0, "2025-07-01"),
    (13, 12, "Laptop", 1, 1250.0, "2025-07-10"),
    (14, 13, "Mouse", 2, 26.0, "2025-07-15"),
    (15, 14, "Keyboard", 1, 42.0, "2025-08-01"),
]


def assert_rows_equal(rows_0: list[Row], rows_1: list[Row]):
    # sort rows by all keys to ensure order doesn't matter
    rows_0 = sorted(rows_0, key=lambda r: tuple(r.values()))
    rows_1 = sorted(rows_1, key=lambda r: tuple(r.values()))
    for r0, r1 in zip(rows_0, rows_1, strict=True):
        assert r0.keys() == r1.keys(), f"Row keys mismatch: {r0.keys()} != {r1.keys()}"
        for key in r0:
            assert r0[key] == r1[key], f"Row value mismatch for key '{key}': {r0[key]} != {r1[key]}"
            assert type(r0[key]) is type(r1[key]), (
                f"Row value type mismatch for key '{key}': {type(r0[key])} != {type(r1[key])}"
            )


@pytest.fixture(autouse=True)
def test_data_users(tmp_path: Path):
    schema = [("user_id", INT), ("first_name", STR), ("last_name", STR), ("age", INT), ("country", STR)]
    column_data = tuple(map(list, zip(*USERS, strict=True)))
    BlockFile(tmp_path / "users", schema).write_data(column_data)


@pytest.fixture(autouse=True)
def test_data_orders(tmp_path: Path):
    schema = [
        ("order_id", INT),
        ("user_id", INT),
        ("product", STR),
        ("quantity", INT),
        ("price", FLOAT),
        ("order_date", STR),
    ]
    column_data = tuple(map(list, zip(*ORDERS, strict=True)))
    BlockFile(tmp_path / "orders", schema).write_data(column_data)


@pytest.fixture
def test_tables(tmp_path: Path) -> tuple[str, str]:
    return (str(tmp_path / "users"), str(tmp_path / "orders"))


def to_rows(schema: tuple[str, ...], rows: list[tuple[Any, ...]]) -> list[Row]:
    return [dict(zip(schema, row, strict=True)) for row in rows]


@pytest.mark.parametrize(
    ("query", "expected_rows"),
    [
        (
            "SELECT * FROM '{users}';",
            to_rows(
                ("user_id", "first_name", "last_name", "age", "country"),
                [
                    (1, "Alice", "Smith", 25, "USA"),
                    (2, "Bob", "Johnson", 30, "Canada"),
                    (3, "Charlie", "Brown", 22, "USA"),
                    (4, "David", "Wilson", 35, "UK"),
                    (5, "Eva", "Davis", 28, "Canada"),
                    (6, "Frank", "Miller", 40, "USA"),
                    (7, "Grace", "Taylor", 27, "UK"),
                    (8, "Hank", "Anderson", 32, "USA"),
                    (9, "Ivy", "Thomas", 26, "Canada"),
                    (10, "Jack", "Jackson", 24, "USA"),
                    (11, "Kate", "White", 29, "UK"),
                    (12, "Leo", "Harris", 33, "USA"),
                    (13, "Mia", "Martin", 31, "Canada"),
                    (14, "Nick", "Thompson", 23, "UK"),
                    (15, "Olivia", "Garcia", 36, "USA"),
                ],
            ),
        ),
        (
            "SELECT first_name, last_name FROM '{users}' WHERE country='USA';",
            to_rows(
                ("first_name", "last_name"),
                [
                    ("Alice", "Smith"),
                    ("Charlie", "Brown"),
                    ("Frank", "Miller"),
                    ("Hank", "Anderson"),
                    ("Jack", "Jackson"),
                    ("Leo", "Harris"),
                    ("Olivia", "Garcia"),
                ],
            ),
        ),
        (
            "SELECT first_name + ' ' + last_name AS full_name FROM '{users}';",
            to_rows(
                ("full_name",),
                [
                    ("Alice Smith",),
                    ("Bob Johnson",),
                    ("Charlie Brown",),
                    ("David Wilson",),
                    ("Eva Davis",),
                    ("Frank Miller",),
                    ("Grace Taylor",),
                    ("Hank Anderson",),
                    ("Ivy Thomas",),
                    ("Jack Jackson",),
                    ("Kate White",),
                    ("Leo Harris",),
                    ("Mia Martin",),
                    ("Nick Thompson",),
                    ("Olivia Garcia",),
                ],
            ),
        ),
        (
            "SELECT user_id, age, age+5 AS age_in_5_years FROM '{users}';",
            to_rows(
                ("user_id", "age", "age_in_5_years"),
                [
                    (1, 25, 30),
                    (2, 30, 35),
                    (3, 22, 27),
                    (4, 35, 40),
                    (5, 28, 33),
                    (6, 40, 45),
                    (7, 27, 32),
                    (8, 32, 37),
                    (9, 26, 31),
                    (10, 24, 29),
                    (11, 29, 34),
                    (12, 33, 38),
                    (13, 31, 36),
                    (14, 23, 28),
                    (15, 36, 41),
                ],
            ),
        ),
        (
            "SELECT * FROM '{orders}' WHERE price > 100;",
            to_rows(
                ("order_id", "user_id", "product", "quantity", "price", "order_date"),
                [
                    (1, 1, "Laptop", 1, 1200.0, "2025-01-01"),
                    (4, 1, "Monitor", 2, 300.0, "2025-03-15"),
                    (5, 4, "Laptop", 1, 1100.0, "2025-03-20"),
                    (8, 7, "Monitor", 1, 280.0, "2025-05-05"),
                    (9, 8, "Laptop", 1, 1300.0, "2025-05-10"),
                    (12, 11, "Monitor", 2, 290.0, "2025-07-01"),
                    (13, 12, "Laptop", 1, 1250.0, "2025-07-10"),
                ],
            ),
        ),
        (
            "SELECT product, quantity*price AS total_value FROM '{orders}';",
            to_rows(
                ("product", "total_value"),
                [
                    ("Laptop", 1200.0),
                    ("Mouse", 50.0),
                    ("Keyboard", 45.0),
                    ("Monitor", 600.0),
                    ("Laptop", 1100.0),
                    ("Mouse", 30.0),
                    ("Keyboard", 100.0),
                    ("Monitor", 280.0),
                    ("Laptop", 1300.0),
                    ("Mouse", 81.0),
                    ("Keyboard", 40.0),
                    ("Monitor", 580.0),
                    ("Laptop", 1250.0),
                    ("Mouse", 52.0),
                    ("Keyboard", 42.0),
                ],
            ),
        ),
        (
            "SELECT * FROM '{orders}' WHERE order_date BETWEEN '2025-03-01' AND '2025-06-01';",
            to_rows(
                ("order_id", "user_id", "product", "quantity", "price", "order_date"),
                [
                    (4, 1, "Monitor", 2, 300.0, "2025-03-15"),
                    (5, 4, "Laptop", 1, 1100.0, "2025-03-20"),
                    (6, 5, "Mouse", 1, 30.0, "2025-04-01"),
                    (7, 6, "Keyboard", 2, 50.0, "2025-04-10"),
                    (8, 7, "Monitor", 1, 280.0, "2025-05-05"),
                    (9, 8, "Laptop", 1, 1300.0, "2025-05-10"),
                    (10, 9, "Mouse", 3, 27.0, "2025-06-01"),
                ],
            ),
        ),
        (
            "SELECT * FROM '{orders}' WHERE product LIKE '%top%';",
            to_rows(
                ("order_id", "user_id", "product", "quantity", "price", "order_date"),
                [
                    (1, 1, "Laptop", 1, 1200.0, "2025-01-01"),
                    (5, 4, "Laptop", 1, 1100.0, "2025-03-20"),
                    (9, 8, "Laptop", 1, 1300.0, "2025-05-10"),
                    (13, 12, "Laptop", 1, 1250.0, "2025-07-10"),
                ],
            ),
        ),
        (
            "SELECT country, COUNT() AS user_count FROM '{users}' GROUP BY country;",
            to_rows(
                ("country", "user_count"),
                [
                    ("USA", 7),
                    ("Canada", 4),
                    ("UK", 4),
                ],
            ),
        ),
        (
            "SELECT user_id, SUM(quantity*price) AS total_spent FROM '{orders}' GROUP BY user_id;",
            to_rows(
                ("user_id", "total_spent"),
                [
                    (1, 1200.0 + 600.0),  # Laptop + Monitor
                    (2, 50.0),
                    (3, 45.0),
                    (4, 1100.0),
                    (5, 30.0),
                    (6, 100.0),
                    (7, 280.0),
                    (8, 1300.0),
                    (9, 81.0),
                    (10, 40.0),
                    (11, 580.0),
                    (12, 1250.0),
                    (13, 52.0),
                    (14, 42.0),
                ],
            ),
        ),
        (
            "SELECT product, AVG(price) AS avg_price FROM '{orders}' GROUP BY product;",
            to_rows(
                ("product", "avg_price"),
                [
                    ("Laptop", (1200 + 1100 + 1300 + 1250) / 4),
                    ("Mouse", (25 + 30 + 27 + 26) / 4),
                    ("Keyboard", (45 + 50 + 40 + 42) / 4),
                    ("Monitor", (300 + 280 + 290) / 3),
                ],
            ),
        ),
        (
            "SELECT country, AVG(age) AS avg_age FROM '{users}' GROUP BY country;",
            to_rows(
                ("country", "avg_age"),
                [
                    ("USA", (25 + 22 + 40 + 32 + 24 + 33 + 36) / 7),
                    ("Canada", (30 + 28 + 26 + 31) / 4),
                    ("UK", (35 + 27 + 29 + 23) / 4),
                ],
            ),
        ),
        (
            "SELECT user_id, COUNT() AS order_count FROM '{orders}' GROUP BY user_id HAVING COUNT() > 1;",
            to_rows(
                ("user_id", "order_count"),
                [
                    (1, 2),
                ],
            ),
        ),
        (
            "SELECT u.first_name, o.product FROM '{users}' AS u JOIN '{orders}' AS o ON u.user_id=o.user_id;",
            to_rows(
                ("first_name", "product"),
                [
                    ("Alice", "Laptop"),
                    ("Alice", "Monitor"),
                    ("Bob", "Mouse"),
                    ("Charlie", "Keyboard"),
                    ("David", "Laptop"),
                    ("Eva", "Mouse"),
                    ("Frank", "Keyboard"),
                    ("Grace", "Monitor"),
                    ("Hank", "Laptop"),
                    ("Ivy", "Mouse"),
                    ("Jack", "Keyboard"),
                    ("Kate", "Monitor"),
                    ("Leo", "Laptop"),
                    ("Mia", "Mouse"),
                    ("Nick", "Keyboard"),
                ],
            ),
        ),
        (
            "SELECT u.country, COUNT() AS orders_count "
            "FROM '{users}' AS u JOIN '{orders}' AS o ON u.user_id=o.user_id GROUP BY u.country;",
            to_rows(
                ("country", "orders_count"),
                [
                    ("USA", 7),
                    ("Canada", 4),
                    ("UK", 4),
                ],
            ),
        ),
        (
            "SELECT u.first_name, SUM(o.quantity*o.price) AS spent "
            "FROM '{users}' AS u JOIN '{orders}' AS o ON u.user_id=o.user_id GROUP BY u.first_name;",
            to_rows(
                ("first_name", "spent"),
                [
                    ("Alice", 1200.0 + 600.0),
                    ("Bob", 50.0),
                    ("Charlie", 45.0),
                    ("David", 1100.0),
                    ("Eva", 30.0),
                    ("Frank", 100.0),
                    ("Grace", 280.0),
                    ("Hank", 1300.0),
                    ("Ivy", 81.0),
                    ("Jack", 40.0),
                    ("Kate", 580.0),
                    ("Leo", 1250.0),
                    ("Mia", 52.0),
                    ("Nick", 42.0),
                ],
            ),
        ),
        (
            "SELECT u.first_name, o.product, o.price "
            "FROM '{users}' AS u LEFT JOIN '{orders}' AS o ON u.user_id=o.user_id WHERE o.price > 100;",
            to_rows(
                ("first_name", "product", "price"),
                [
                    ("Alice", "Laptop", 1200.0),
                    ("Alice", "Monitor", 300.0),
                    ("David", "Laptop", 1100.0),
                    ("Grace", "Monitor", 280.0),
                    ("Hank", "Laptop", 1300.0),
                    ("Kate", "Monitor", 290.0),
                    ("Leo", "Laptop", 1250.0),
                ],
            ),
        ),
        (
            "SELECT u.first_name, o.product, o.order_date "
            "FROM '{orders}' AS o LEFT JOIN '{users}' AS u ON u.user_id=o.user_id WHERE o.order_date > '2025-05-01';",
            to_rows(
                ("first_name", "product", "order_date"),
                [
                    ("Hank", "Laptop", "2025-05-10"),
                    ("Ivy", "Mouse", "2025-06-01"),
                    ("Jack", "Keyboard", "2025-06-15"),
                    ("Kate", "Monitor", "2025-07-01"),
                    ("Leo", "Laptop", "2025-07-10"),
                    ("Mia", "Mouse", "2025-07-15"),
                    ("Nick", "Keyboard", "2025-08-01"),
                ],
            ),
        ),
        (
            "SELECT product, SUM(quantity) AS total_quantity, MAX(price) AS max_price FROM '{orders}' "
            "GROUP BY product;",
            to_rows(
                ("product", "total_quantity", "max_price"),
                [
                    ("Laptop", 4, 1300.0),
                    ("Mouse", 8, 30.0),
                    ("Keyboard", 5, 50.0),
                    ("Monitor", 5, 300.0),
                ],
            ),
        ),
        (
            "SELECT u.country, COUNT() AS orders_count, SUM(o.quantity*o.price) AS total_sales "
            "FROM '{users}' AS u JOIN '{orders}' AS o ON u.user_id=o.user_id GROUP BY u.country "
            "HAVING SUM(o.quantity*o.price) > 500;",
            to_rows(
                ("country", "orders_count", "total_sales"),
                [
                    ("USA", 7, float(1 * 1200 + 1 * 45 + 2 * 300 + 2 * 50 + 1 * 1300 + 1 * 40 + 1 * 1250)),
                    ("UK", 4, float(1 * 1100 + 1 * 280 + 2 * 290 + 1 * 42)),
                    # ("Canada", 4, 213) ignored total_sales <= 500
                ],
            ),
        ),
    ],
)
@pytest.mark.parametrize("engine_factory", ENGINES)
def test_full_query(
    test_tables: tuple[str, str],
    engine_factory: type[ExecutionEngine],
    query: str,
    expected_rows: list[Row],
):
    users, products = test_tables
    with engine_factory() as engine:
        df = parse_sql(query.format(users=users, orders=products))
        df.task.explain()
        PhysicalPlan.generate_physical_plan(deepcopy(df.task)).explain()
        df.engine = engine
        rows = df.collect()

    assert_rows_equal(rows, expected_rows)
