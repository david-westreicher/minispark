from mini_spark.sql import ColumnType, Col, Lit
from mini_spark.tasks import (
    GroupByTask,
    JoinTask,
    FilterTask,
    ProjectTask,
    LoadTableTask,
    ShuffleToFileTask,
    VoidTask,
    BlockFile,
    LoadShuffleFileTask,
)
import pytest
from pathlib import Path
from unittest.mock import patch


def test_schema_propagation_load_table():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = LoadTableTask(VoidTask(), file_path=Path())

    # act
    with patch.object(
        BlockFile, "file_schema", property(lambda self: test_table_schema)
    ):
        schema = task.validate_schema()

    assert schema == test_table_schema


def test_schema_propagation_projection():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = ProjectTask(
        LoadTableTask(VoidTask(), file_path=Path()),
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
        BlockFile, "file_schema", property(lambda self: test_table_schema)
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
        LoadTableTask(VoidTask(), file_path=Path()),
        columns=[
            Col("a") + Col("b"),
        ],
    )

    # act/assert
    with (
        patch.object(
            BlockFile,
            "file_schema",
            property(lambda self: test_table_schema),
        ),
        pytest.raises(TypeError, match="Type mismatch in binary operation"),
    ):
        task.validate_schema()


def test_schema_propagation_projection_select_star():
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = ProjectTask(
        LoadTableTask(VoidTask(), file_path=Path()),
        columns=[
            Col("a").alias("start"),
            Col("*"),
            Col("b").alias("finish"),
        ],
    )

    # act
    with patch.object(
        BlockFile, "file_schema", property(lambda self: test_table_schema)
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
    task = LoadShuffleFileTask(LoadTableTask(VoidTask(), file_path=Path()))

    # act
    with patch.object(
        BlockFile, "file_schema", property(lambda self: test_table_schema)
    ):
        schema = task.validate_schema()

    assert schema == test_table_schema


def test_schema_propagation_filter_task():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = FilterTask(
        LoadTableTask(VoidTask(), file_path=Path()),
        column=Col("a") > Col("b"),
    )

    # act
    with patch.object(
        BlockFile, "file_schema", property(lambda self: test_table_schema)
    ):
        schema = task.validate_schema()

    assert schema == test_table_schema


def test_schema_propagation_filter_task_fails():
    # arrange
    test_table_schema = [("a", ColumnType.INTEGER), ("b", ColumnType.STRING)]
    task = FilterTask(
        LoadTableTask(VoidTask(), file_path=Path()),
        column=Col("c") > Lit(5),
    )

    # act / assert
    with (
        patch.object(
            BlockFile, "file_schema", property(lambda self: test_table_schema)
        ),
        pytest.raises(ValueError, match="Unknown columns in Filter"),
    ):
        task.validate_schema()


def test_schema_propagation_join_task():
    # arrange
    test_table_schema = [("left_id", ColumnType.STRING), ("b", ColumnType.INTEGER)]
    task = JoinTask(
        ShuffleToFileTask(GroupByTask(LoadTableTask(VoidTask(), file_path=Path()))),
        right_side_task=ShuffleToFileTask(
            GroupByTask(
                ProjectTask(
                    LoadTableTask(VoidTask(), file_path=Path()),
                    columns=[
                        Col("left_id").alias("right_id"),
                        Col("b").alias("c"),
                    ],
                )
            )
        ),
        join_condition=Col("left_id") == Col("right_id"),
    )

    # act
    with patch.object(
        BlockFile, "file_schema", property(lambda self: test_table_schema)
    ):
        schema = task.validate_schema()

    assert schema == [
        ("left_id", ColumnType.STRING),
        ("b", ColumnType.INTEGER),
        ("right_id", ColumnType.STRING),
        ("c", ColumnType.INTEGER),
    ]
