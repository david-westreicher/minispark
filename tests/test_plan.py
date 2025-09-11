from pathlib import Path

import pytest

from mini_spark.constants import ColumnType
from mini_spark.io import BlockFile
from mini_spark.plan import PhysicalPlan
from mini_spark.sql import Col, Lit
from mini_spark.sql import Functions as F  # noqa: N817
from mini_spark.tasks import (
    AggregateCountTask,
    AggregateTask,
    FilterTask,
    JoinTask,
    LoadShuffleFilesTask,
    LoadTableBlockTask,
    ProjectTask,
    VoidTask,
    WriteToLocalFileTask,
    WriteToShufflePartitions,
)


@pytest.fixture
def test_data_file(tmp_path: Path) -> str:
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


def test_physical_plan_infer_load_table(test_data_file: str):
    # arrange
    task = LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file))

    # act
    PhysicalPlan.infer_schema(task)

    # assert
    assert task.inferred_schema == [
        ("fruit", ColumnType.STRING),
        ("quantity", ColumnType.INTEGER),
        ("color", ColumnType.STRING),
    ]


def test_physical_plan_infer_project(test_data_file: str):
    # arrange
    task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        columns=[
            Col("fruit"),
            (Col("fruit") + Lit("other")).alias("str_concat"),
            Col("quantity"),
            (Col("quantity") * 2).alias("binop"),
            Lit(2).alias("int_lit"),
            Lit("str").alias("str_lit"),
        ],
    )

    # act
    PhysicalPlan.infer_schema(task)

    # assert
    assert task.inferred_schema == [
        ("fruit", ColumnType.STRING),
        ("str_concat", ColumnType.STRING),
        ("quantity", ColumnType.INTEGER),
        ("binop", ColumnType.INTEGER),
        ("int_lit", ColumnType.INTEGER),
        ("str_lit", ColumnType.STRING),
    ]


def test_physical_plan_infer_filter(test_data_file: str):
    # arrange
    task = FilterTask(LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)), condition=Col("fruit") == "apple")

    # act
    PhysicalPlan.infer_schema(task)

    # assert
    assert task.inferred_schema == [
        ("fruit", ColumnType.STRING),
        ("quantity", ColumnType.INTEGER),
        ("color", ColumnType.STRING),
    ]


def test_physical_plan_infer_join(test_data_file: str):
    # arrange
    left_task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        columns=[Col("fruit").alias("fruit_left"), Col("quantity")],
    )
    right_task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        columns=[Col("fruit").alias("fruit_right"), Col("color")],
    )
    task = JoinTask(
        left_task,
        right_side_task=right_task,
        join_condition=Col("fruit_left") == Col("fruit_right"),
        how="inner",
    )

    # act
    PhysicalPlan.infer_schema(task)

    # assert
    assert task.inferred_schema == [
        ("fruit_left", ColumnType.STRING),
        ("quantity", ColumnType.INTEGER),
        ("fruit_right", ColumnType.STRING),
        ("color", ColumnType.STRING),
    ]


def test_physical_plan_infer_count(test_data_file: str):
    # arrange
    task = AggregateCountTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)), group_by_column=Col("fruit")
    )

    # act
    PhysicalPlan.infer_schema(task)

    # assert
    assert task.inferred_schema == [
        ("fruit", ColumnType.STRING),
        ("count", ColumnType.INTEGER),
    ]


def test_physical_plan_expand_join(test_data_file: str):
    # arrange
    left_task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        columns=[Col("fruit").alias("fruit_left"), Col("quantity")],
    )
    right_task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        columns=[Col("fruit").alias("fruit_right"), Col("color")],
    )
    task = JoinTask(
        left_task,
        right_side_task=right_task,
        join_condition=Col("fruit_left") == Col("fruit_right"),
        how="inner",
    )

    # act
    PhysicalPlan.expand_tasks(task)

    # assert
    assert type(task) is JoinTask
    left_task_types = [type(t) for t in task.parent_task.task_chain]
    right_task_types = [type(t) for t in task.right_side_task.task_chain]
    assert left_task_types == right_task_types == [LoadTableBlockTask, ProjectTask, WriteToShufflePartitions]
    assert type(task.parent_task) is WriteToShufflePartitions
    assert task.parent_task.key_column == Col("fruit_left")
    assert type(task.right_side_task) is WriteToShufflePartitions
    assert task.right_side_task.key_column == Col("fruit_right")


def test_physical_plan_expand_count(test_data_file: str):
    # arrange
    task = AggregateCountTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)), group_by_column=Col("fruit")
    )

    # act
    PhysicalPlan.expand_tasks(task)

    # assert
    task_types = [type(t) for t in task.task_chain]
    assert task_types == [
        LoadTableBlockTask,
        AggregateCountTask,
        WriteToShufflePartitions,
        LoadShuffleFilesTask,
        AggregateCountTask,
    ]
    assert task.in_sum_mode


def test_physical_plan_expand_agg(test_data_file: str):
    # arrange
    task = AggregateTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        group_by_column=Col("fruit"),
        agg_columns=[F.min(Col("quantity"))],
    )

    # act
    PhysicalPlan.expand_tasks(task)

    # assert
    task_types = [type(t) for t in task.task_chain]
    assert task_types == [
        LoadTableBlockTask,
        AggregateTask,
        WriteToShufflePartitions,
        LoadShuffleFilesTask,
        AggregateTask,
    ]
    assert not task.before_shuffle


def test_physical_plan_generate_join(test_data_file: str):
    # arrange
    left_task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        columns=[Col("fruit").alias("fruit_left"), Col("quantity")],
    )
    right_task = ProjectTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)),
        columns=[Col("fruit").alias("fruit_right"), Col("color")],
    )
    task = JoinTask(
        left_task,
        right_side_task=right_task,
        join_condition=Col("fruit_left") == Col("fruit_right"),
        how="inner",
    )

    # act
    stages = PhysicalPlan.generate_physical_plan(task).stages

    # assert
    stage_0, stage_1, stage_2 = stages

    assert type(stage_0.producer) is LoadTableBlockTask
    assert [type(c) for c in stage_0.consumers] == [ProjectTask]
    assert type(stage_0.writer) is WriteToShufflePartitions

    assert type(stage_1.producer) is LoadTableBlockTask
    assert [type(c) for c in stage_1.consumers] == [ProjectTask]
    assert type(stage_1.writer) is WriteToShufflePartitions

    assert type(stage_2.producer) is JoinTask
    assert [type(c) for c in stage_2.consumers] == []
    assert type(stage_2.writer) is WriteToLocalFileTask


def test_physical_plan_generate_count(test_data_file: str):
    # arrange
    task = AggregateCountTask(
        LoadTableBlockTask(VoidTask(), file_path=Path(test_data_file)), group_by_column=Col("fruit")
    )

    # act
    stages = PhysicalPlan.generate_physical_plan(task).stages

    # assert
    stage_0, stage_1 = stages

    assert type(stage_0.producer) is LoadTableBlockTask
    assert [type(c) for c in stage_0.consumers] == [AggregateCountTask]
    assert type(stage_0.writer) is WriteToShufflePartitions

    assert type(stage_1.producer) is LoadShuffleFilesTask
    assert [type(c) for c in stage_1.consumers] == [AggregateCountTask]
    assert type(stage_1.writer) is WriteToLocalFileTask
