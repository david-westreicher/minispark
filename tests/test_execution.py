from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from mini_spark.dataframe import DataFrame
from mini_spark.execution import ExecutionEngine, PythonExecutionEngine, ThreadEngine
from mini_spark.io import BlockFile
from mini_spark.sql import Col
from mini_spark.sql import Functions as F  # noqa: N817

if TYPE_CHECKING:
    from mini_spark.constants import Row


@pytest.fixture
def test_data(tmp_path: Path) -> str:
    test_file = tmp_path / "fruits.bin"
    test_data: list[Row] = [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]
    BlockFile(test_file).write_rows(test_data)
    return str(test_file)


ENGINES = [PythonExecutionEngine, ThreadEngine]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_table_load(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = DataFrame(engine).table(test_data).collect()

    # assert
    assert rows == [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_engine_sql(test_data: str, engine_factory: type[ExecutionEngine]):
    # arrange
    query = f"SELECT * FROM '{test_data}';"  # noqa: S608

    # act
    with engine_factory() as engine:
        rows = engine.sql(query).collect()

    # assert
    assert rows == [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_select(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = DataFrame(engine).table(test_data).select(Col("fruit")).collect()

    # assert
    assert rows == [
        {"fruit": "apple"},
        {"fruit": "banana"},
        {"fruit": "orange"},
        {"fruit": "apple"},
        {"fruit": "banana"},
    ]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_select_expression(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = DataFrame(engine).table(test_data).select(Col("quantity") + 3).collect()

    # assert
    assert rows == [
        {"quantity_add_lit_3": 6},
        {"quantity_add_lit_3": 8},
        {"quantity_add_lit_3": 5},
        {"quantity_add_lit_3": 7},
        {"quantity_add_lit_3": 10},
    ]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_select_alias(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = DataFrame(engine).table(test_data).select(Col("fruit").alias("fruit_name")).collect()

    # assert
    assert rows == [
        {"fruit_name": "apple"},
        {"fruit_name": "banana"},
        {"fruit_name": "orange"},
        {"fruit_name": "apple"},
        {"fruit_name": "banana"},
    ]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_select_star(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = DataFrame(engine).table(test_data).select(Col("*")).collect()

    # assert
    assert rows == [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_filter(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = DataFrame(engine).table(test_data).filter(Col("quantity") > 3).collect()

    # assert
    assert rows == [
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_groupby_single_col_single_agg(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = DataFrame(engine).table(test_data).group_by(Col("fruit")).agg(F.count()).collect()

    # assert
    # TODO(david): implement assertDataFrameEquals
    expected_rows = [
        {"fruit": "apple", "count": 2},
        {"fruit": "banana", "count": 2},
        {"fruit": "orange", "count": 1},
    ]
    sort_key = lambda row: row["fruit"]  # noqa: E731
    rows.sort(key=sort_key)
    expected_rows.sort(key=sort_key)
    assert rows == expected_rows


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_groupby_single_col_multiple_agg(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = (
            DataFrame(engine)
            .table(test_data)
            .group_by(Col("fruit"))
            .agg(
                F.count(),
                F.min(Col("quantity")).alias("min"),
                F.max(Col("quantity")).alias("max"),
                F.sum(Col("quantity")).alias("sum"),
            )
            .collect()
        )

    # assert
    expected_rows = [
        {
            "fruit": "apple",
            "count": 2,
            "min": 3,
            "max": 4,
            "sum": 7,
        },
        {
            "fruit": "banana",
            "count": 2,
            "min": 5,
            "max": 7,
            "sum": 12,
        },
        {
            "fruit": "orange",
            "count": 1,
            "min": 2,
            "max": 2,
            "sum": 2,
        },
    ]
    sort_key = lambda row: row["fruit"]  # noqa: E731
    rows.sort(key=sort_key)
    expected_rows.sort(key=sort_key)
    assert rows == expected_rows


@pytest.mark.parametrize("engine_factory", ENGINES)
def test_join(test_data: str, engine_factory: type[ExecutionEngine]):
    # act
    with engine_factory() as engine:
        rows = (
            DataFrame(engine)
            .table(test_data)
            .select(Col("fruit").alias("fruit_left"), Col("color"))
            .join(
                DataFrame().table(test_data).select(Col("fruit").alias("fruit_right"), Col("quantity")),
                on=Col("fruit_left") == Col("fruit_right"),
                how="inner",
            )
            .collect()
        )

    # assert
    expected_rows = [
        {
            "fruit_left": "apple",
            "fruit_right": "apple",
            "color": "green",
            "quantity": 3,
        },
        {
            "fruit_left": "apple",
            "fruit_right": "apple",
            "color": "red",
            "quantity": 3,
        },
        {
            "fruit_left": "apple",
            "fruit_right": "apple",
            "color": "green",
            "quantity": 4,
        },
        {
            "fruit_left": "apple",
            "fruit_right": "apple",
            "color": "red",
            "quantity": 4,
        },
        {
            "fruit_left": "banana",
            "fruit_right": "banana",
            "color": "yellow",
            "quantity": 5,
        },
        {
            "fruit_left": "banana",
            "fruit_right": "banana",
            "color": "yellow",
            "quantity": 7,
        },
        {
            "fruit_left": "banana",
            "fruit_right": "banana",
            "color": "yellow",
            "quantity": 5,
        },
        {
            "fruit_left": "banana",
            "fruit_right": "banana",
            "color": "yellow",
            "quantity": 7,
        },
        {
            "fruit_left": "orange",
            "fruit_right": "orange",
            "color": "orange",
            "quantity": 2,
        },
    ]
    sort_key = lambda row: (row["fruit_left"], row["color"], row["quantity"])  # noqa: E731
    rows.sort(key=sort_key)
    expected_rows.sort(key=sort_key)
    assert rows == expected_rows
