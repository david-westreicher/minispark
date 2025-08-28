from pathlib import Path

import pytest

from mini_spark.dataframe import Col, DataFrame
from mini_spark.io import BlockFile


@pytest.fixture
def test_data(tmp_path: Path) -> str:
    test_file = tmp_path / "fruits.bin"
    test_data = [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]
    BlockFile(test_file).write_rows(test_data)
    return str(test_file)


def test_table_load(test_data: str):
    # act
    rows = DataFrame().table(test_data).collect()

    # assert
    assert rows == [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]


def test_select(test_data: str):
    # act
    rows = DataFrame().table(test_data).select(Col("fruit")).collect()

    # assert
    assert rows == [
        {"fruit": "apple"},
        {"fruit": "banana"},
        {"fruit": "orange"},
        {"fruit": "apple"},
        {"fruit": "banana"},
    ]


def test_select_expression(test_data: str):
    # act
    rows = DataFrame().table(test_data).select(Col("quantity") + 3).collect()

    # assert
    assert rows == [
        {"quantity_add_lit_3": 6},
        {"quantity_add_lit_3": 8},
        {"quantity_add_lit_3": 5},
        {"quantity_add_lit_3": 7},
        {"quantity_add_lit_3": 10},
    ]


def test_select_alias(test_data: str):
    # act
    rows = (
        DataFrame().table(test_data).select(Col("fruit").alias("fruit_name")).collect()
    )

    # assert
    assert rows == [
        {"fruit_name": "apple"},
        {"fruit_name": "banana"},
        {"fruit_name": "orange"},
        {"fruit_name": "apple"},
        {"fruit_name": "banana"},
    ]


def test_select_star(test_data: str):
    # act
    rows = DataFrame().table(test_data).select(Col("*")).collect()

    # assert
    assert rows == [
        {"fruit": "apple", "quantity": 3, "color": "red"},
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "orange", "quantity": 2, "color": "orange"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]


def test_filter(test_data: str):
    # act
    rows = DataFrame().table(test_data).filter(Col("quantity") > 3).collect()

    # assert
    assert rows == [
        {"fruit": "banana", "quantity": 5, "color": "yellow"},
        {"fruit": "apple", "quantity": 4, "color": "green"},
        {"fruit": "banana", "quantity": 7, "color": "yellow"},
    ]


def test_groupby(test_data: str):
    # act
    rows = DataFrame().table(test_data).group_by(Col("fruit")).count().collect()

    # assert
    # TODO: implement assertDataFrameEquals
    expected_rows = [
        {"fruit": "apple", "count": 2},
        {"fruit": "banana", "count": 2},
        {"fruit": "orange", "count": 1},
    ]
    sort_key = lambda row: row["fruit"]  # noqa: E731
    rows.sort(key=sort_key)
    expected_rows.sort(key=sort_key)
    assert rows == expected_rows


def test_join(test_data: str):
    # act
    rows = (
        DataFrame()
        .table(test_data)
        .select(Col("fruit").alias("fruit_left"), Col("color"))
        .join(
            DataFrame()
            .table(test_data)
            .select(Col("fruit").alias("fruit_right"), Col("quantity")),
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
