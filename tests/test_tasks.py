from pathlib import Path
from unittest.mock import patch

import pytest

from mini_spark.constants import ColumnType
from mini_spark.io import BlockFile
from mini_spark.sql import Col, Lit
from mini_spark.sql import Functions as F  # noqa: N817
from mini_spark.tasks import (
    AggregateTask,
    FilterTask,
    JoinTask,
    LoadShuffleFilesTask,
    LoadTableBlockTask,
    ProjectTask,
    VoidTask,
    WriteToShufflePartitions,
)


def test_schema_propagation_load_table():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = LoadTableBlockTask(VoidTask(), file_path=Path())

    # act
    with patch.object(
        BlockFile,
        "file_schema",
        property(lambda _: test_table_schema),
    ):
        schema = task.validate_schema()

    assert schema == test_table_schema


def test_schema_propagation_projection():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path()),
        columns=[
            Col("a"),
            Col("b"),
            Lit(1).alias("num"),
            Lit("str").alias("str"),
            Col("a") + Lit(1),
            (Col("a") - Lit(1)).alias("c"),
        ],
    )

    # act
    with patch.object(
        BlockFile,
        "file_schema",
        property(lambda _: test_table_schema),
    ):
        schema = task.validate_schema()

    # assert
    assert schema == [
        ("a", ColumnType.INTEGER),
        ("b", ColumnType.STRING),
        ("num", ColumnType.INTEGER),
        ("str", ColumnType.STRING),
        ("a_add_lit_1", ColumnType.INTEGER),
        ("c", ColumnType.INTEGER),
    ]


def test_schema_propagation_projection_type_mismatch():
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path()),
        columns=[
            Col("a") + Col("b"),
        ],
    )

    # act/assert
    with (
        patch.object(
            BlockFile,
            "file_schema",
            property(lambda _: test_table_schema),
        ),
        pytest.raises(TypeError, match="Type mismatch in binary operation"),
    ):
        task.validate_schema()


def test_schema_propagation_projection_select_star():
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path()),
        columns=[
            Col("a").alias("start"),
            Col("*"),
            Col("b").alias("finish"),
        ],
    )

    # act
    with patch.object(
        BlockFile,
        "file_schema",
        property(lambda _: test_table_schema),
    ):
        schema = task.validate_schema()

    # assert
    assert schema == [
        ("start", ColumnType.INTEGER),
        ("a", ColumnType.INTEGER),
        ("b", ColumnType.STRING),
        ("finish", ColumnType.STRING),
    ]


def test_schema_propagation_load_shuffle_file_task():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = LoadShuffleFilesTask(LoadTableBlockTask(VoidTask(), file_path=Path()))

    # act
    with patch.object(
        BlockFile,
        "file_schema",
        property(lambda _: test_table_schema),
    ):
        schema = task.validate_schema()

    assert schema == test_table_schema


def test_schema_propagation_filter_task():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = FilterTask(
        LoadTableBlockTask(VoidTask(), file_path=Path()),
        condition=Col("a") > Col("b"),
    )

    # act
    with patch.object(
        BlockFile,
        "file_schema",
        property(lambda _: test_table_schema),
    ):
        schema = task.validate_schema()

    assert schema == test_table_schema


def test_schema_propagation_filter_task_fails():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = FilterTask(
        LoadTableBlockTask(VoidTask(), file_path=Path()),
        condition=Col("c") > Lit(5),
    )

    # act / assert
    with (
        patch.object(
            BlockFile,
            "file_schema",
            property(lambda _: test_table_schema),
        ),
        pytest.raises(ValueError, match="Unknown columns in Filter"),
    ):
        task.validate_schema()


def test_schema_propagation_join_task():
    # arrange
    test_table_schema = [("left_id", ColumnType.STRING), ("b", ColumnType.INTEGER)]
    task = JoinTask(
        WriteToShufflePartitions(LoadTableBlockTask(VoidTask(), file_path=Path())),
        right_side_task=WriteToShufflePartitions(
            ProjectTask(
                LoadTableBlockTask(VoidTask(), file_path=Path()),
                columns=[
                    Col("left_id").alias("right_id"),
                    Col("b").alias("c"),
                ],
            ),
        ),
        join_condition=Col("left_id") == Col("right_id"),
    )

    # act
    with patch.object(BlockFile, "file_schema", property(lambda _: test_table_schema)):
        schema = task.validate_schema()

    # assert
    assert schema == [
        ("left_id", ColumnType.STRING),
        ("b", ColumnType.INTEGER),
        ("right_id", ColumnType.STRING),
        ("c", ColumnType.INTEGER),
    ]


def test_schema_propagation_aggregation():
    # arrange
    test_table_schema = [("fruit", ColumnType.STRING), ("quantity", ColumnType.INTEGER)]
    task = AggregateTask(
        LoadTableBlockTask(VoidTask(), file_path=Path()),
        group_by_column=Col("fruit"),
        agg_columns=[
            F.count().alias("count_alias"),
            F.count(),
            F.min(Col("quantity")).alias("min"),
            F.min(Col("quantity")),
            F.max(Col("quantity")).alias("max"),
            F.max(Col("quantity")),
            F.sum(Col("quantity")).alias("sum"),
            F.sum(Col("quantity")),
        ],
    )

    # act
    with patch.object(BlockFile, "file_schema", property(lambda _: test_table_schema)):
        schema = task.validate_schema()

    # assert
    assert schema == [
        ("fruit", ColumnType.STRING),
        ("count_alias", ColumnType.INTEGER),
        ("count", ColumnType.INTEGER),
        ("min", ColumnType.INTEGER),
        ("min_quantity", ColumnType.INTEGER),
        ("max", ColumnType.INTEGER),
        ("max_quantity", ColumnType.INTEGER),
        ("sum", ColumnType.INTEGER),
        ("sum_quantity", ColumnType.INTEGER),
    ]


def test_schema_propagation_aggregation_invalid_reference():
    # arrange
    test_table_schema = [("fruit", ColumnType.STRING), ("quantity", ColumnType.INTEGER)]
    task = AggregateTask(
        LoadTableBlockTask(VoidTask(), file_path=Path()),
        group_by_column=Col("fruit"),
        agg_columns=[
            F.min(Col("invalid_reference")),
        ],
    )

    # act / assert
    with (
        patch.object(BlockFile, "file_schema", property(lambda _: test_table_schema)),
        pytest.raises(ValueError, match="Unknown columns in aggregation"),
    ):
        task.validate_schema()
