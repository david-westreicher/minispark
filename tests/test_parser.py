from collections.abc import Callable

import pytest

from mini_spark.dataframe import DataFrame
from mini_spark.parser import parse_sql
from mini_spark.sql import BINOP_SYMBOLS, Col, Lit
from mini_spark.sql import Functions as F  # noqa: N817


@pytest.fixture(autouse=True)
def patch_eq(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    if request.node.get_closest_marker("no_patch_eq"):
        return  # do nothing, skip patching
    monkeypatch.setattr(Col, "__eq__", lambda a, b: repr(a) == repr(b))


def test_select_all():
    # arrange
    sql = """
        SELECT * FROM 'table';
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").select(Col("*"))
    assert df.task == expected_df.task


def test_select_columns():
    # arrange
    sql = """
        SELECT col_1, col_2, col3 AS col_3, *, col_4 FROM 'table';
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .select(
            Col("col_1"),
            Col("col_2"),
            Col("col3").alias("col_3"),
            Col("*"),
            Col("col_4"),
        )
    )
    assert df.task == expected_df.task


def test_select_simple_expression():
    # arrange
    sql = """
        SELECT col_1 - 5 FROM 'table';
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").select(Col("col_1") - 5)
    assert df.task == expected_df.task


def test_select_complex_expression():
    # arrange
    sql = """
        SELECT col_1 - 5 * col_2 / (col_3 + 2) FROM 'table';
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").select(Col("col_1") - Lit(5) * Col("col_2") / (Col("col_3") + 2))
    assert df.task == expected_df.task


def test_where_compare_col_value():
    # arrange
    sql = """
        SELECT * FROM 'table' WHERE col_1 > 100;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").filter(Col("col_1") > Lit(100)).select(Col("*"))
    assert df.task == expected_df.task


def test_where_compare_col_col():
    # arrange
    sql = """
        SELECT * FROM 'table' WHERE col_1 > col_2;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").filter(Col("col_1") > Col("col_2")).select(Col("*"))
    assert df.task == expected_df.task


def test_where_compare_value_col():
    # arrange
    sql = """
        SELECT * FROM 'table' WHERE 100 > col_2;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").filter(Lit(100) > Col("col_2")).select(Col("*"))
    assert df.task == expected_df.task


def test_where_complex_comparison():
    # arrange
    sql = """
        SELECT * FROM 'table' WHERE (100 > col_2) AND ((col_2 < col_3) OR (col_4 != 20));
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .filter((Lit(100) > Col("col_2")) & ((Col("col_2") < Col("col_3")) | (Col("col_4") != Lit(20))))
        .select(Col("*"))
    )
    assert df.task == expected_df.task


def test_where_expression():
    # arrange
    sql = """
        SELECT * FROM 'table' WHERE (col_2 * 10 > col_1 + 2);
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").filter(Col("col_2") * 10 > Col("col_1") + 2).select(Col("*"))
    assert df.task == expected_df.task


@pytest.mark.no_patch_eq
# We need to disable the automatic patching of __eq__ for this test, because we want to test the actual equality op
def test_where_compare_equality(monkeypatch: pytest.MonkeyPatch):
    # arrange
    sql = """
        SELECT * FROM 'table' WHERE col_1 = col_2;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").filter(Col("col_1") == Col("col_2")).select(Col("*"))
    monkeypatch.setattr(Col, "__eq__", lambda a, b: repr(a) == repr(b))
    assert df.task == expected_df.task


@pytest.mark.parametrize(
    ("operator", "op_symbol"),
    [(op, symbol) for op, symbol in BINOP_SYMBOLS.items() if symbol in {"<", "<=", ">=", ">", "!="}],
)
def test_where_compare_operators(operator: Callable[[Col, Col], Col], op_symbol: str):
    # arrange
    sql = f"""
        SELECT * FROM 'table' WHERE col_1 {op_symbol} col_2;
    """  # noqa: S608

    # act
    df = parse_sql(sql)

    # assert
    expected_df = DataFrame().table("table").filter(operator(Col("col_1"), Col("col_2"))).select(Col("*"))
    assert df.task == expected_df.task


def test_groupby_simple():
    # arrange
    sql = """
        SELECT col_1, SUM(col_2) FROM 'table' GROUP BY col_1;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .group_by(Col("col_1"))
        .agg(F.sum(Col("col_2")))
        .select(
            Col("col_1"),
            Col("sum_col_2"),
        )
    )
    assert df.task == expected_df.task


def test_groupby_complex():
    # arrange
    sql = """
        SELECT SUM(col_2), MIN(col_3), MAX(col_4), COUNT() FROM 'table' GROUP BY col_1;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .group_by(Col("col_1"))
        .agg(
            F.sum(Col("col_2")),
            F.min(Col("col_3")),
            F.max(Col("col_4")),
            F.count(),
        )
        .select(
            Col("sum_col_2"),
            Col("min_col_3"),
            Col("max_col_4"),
            Col("count"),
        )
    )
    assert df.task == expected_df.task


def test_groupby_agg_expression():
    # arrange
    sql = """
        SELECT col_1, SUM(col_2 * 2) FROM 'table' GROUP BY col_1;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .group_by(Col("col_1"))
        .agg(
            F.sum(Col("col_2") * 2),
        )
        .select(Col("col_1"), Col("sum_col_2_mul_lit_2"))
    )
    assert df.task == expected_df.task


def test_groupby_agg_alias():
    # arrange
    sql = """
        SELECT col_1, SUM(col_2) AS col_3 FROM 'table' GROUP BY col_1;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .group_by(Col("col_1"))
        .agg(
            F.sum(Col("col_2")).alias("col_3"),
        )
        .select(
            Col("col_1"),
            Col("col_3"),
        )
    )
    assert df.task == expected_df.task


def test_groupby_where():
    # arrange
    sql = """
        SELECT col_1 FROM 'table' WHERE col_1 > col_2 GROUP BY col_1 ;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .filter(Col("col_1") > Col("col_2"))
        .group_by(Col("col_1"))
        .agg()
        .select(
            Col("col_1"),
        )
    )
    assert df.task == expected_df.task


def test_join():
    # arrange
    sql = """
        SELECT col_1, col_2 FROM 'table' JOIN 'table' ON col_1 = col_2;
    """

    # act
    df = parse_sql(sql)

    # assert
    expected_df = (
        DataFrame()
        .table("table")
        .join(DataFrame().table("table"), on=Col("col_1") == Col("col_2"), how="inner")
        .select(
            Col("col_1"),
            Col("col_2"),
        )
    )
    assert df.task == expected_df.task
