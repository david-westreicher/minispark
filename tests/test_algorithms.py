import random
from pathlib import Path
from random import shuffle
from typing import Any, Callable
from unittest.mock import Mock, call

import pytest

from mini_spark.algorithms import (
    SORT_BLOCK_SIZE,
    external_merge_join,
    external_sort,
    kway_merge,
)
from mini_spark.constants import Row
from mini_spark.io import BlockFile


@pytest.mark.parametrize(
    "key_function",
    [lambda row: row["int_col"], lambda row: row["str_col"]],
)
def test_external_sort(tmp_path: Path, key_function: Callable[[Row], Any]):
    # arrange
    input_file = tmp_path / "input.bin"
    tmp_file = tmp_path / "temp.bin"
    output_file = tmp_path / "sorted.bin"
    random.seed(42)
    input_data: list[Row] = [{"int_col": i, "str_col": f"text_{i}"} for i in range(1_000)]
    shuffle(input_data)
    BlockFile(input_file, block_size=SORT_BLOCK_SIZE).write_rows(input_data)

    # act
    external_sort(input_file, key_function, output_file, tmp_file)
    externally_sorted_data = list(BlockFile(output_file).read_data_rows())

    # assert
    sorted_data = sorted(input_data, key=key_function)
    assert externally_sorted_data == sorted_data


def test_kway_merge_action_trigger_1():
    # arrange
    test_data = [[9, 2, 5, 8], [2, 6, 8, 2, 1], [0, -2, 4]]
    iterators = [iter(sorted(data)) for data in test_data]
    result_action = Mock()

    # act
    kway_merge(iterators, key=lambda x: x, action=result_action, action_trigger_size=1)

    # assert
    expected_calls = [
        call([-2]),
        call([0]),
        call([1]),
        call([2]),
        call([2]),
        call([2]),
        call([4]),
        call([5]),
        call([6]),
        call([8]),
        call([8]),
        call([9]),
    ]
    assert result_action.call_args_list == expected_calls


def test_kway_merge_action_trigger_5():
    # arrange
    test_data = [[9, 2, 5, 8], [2, 6, 8, 2, 1], [0, -2, 4]]
    iterators = [iter(sorted(data)) for data in test_data]
    result_action = Mock()

    # act
    kway_merge(iterators, key=lambda x: x, action=result_action, action_trigger_size=5)

    # assert
    expected_calls = [
        call([-2, 0, 1, 2, 2]),
        call([2, 4, 5, 6, 8]),
        call([8, 9]),
    ]
    assert result_action.call_args_list == expected_calls


def test_kway_merge_uncomparable():
    # arrange
    test_data = [[(1, 1 + 2j)], [(1, 3 + 1j)]]
    iterators = [iter(data) for data in test_data]
    result_action = Mock()

    # act
    kway_merge(
        iterators,
        key=lambda x: x[0],
        action=result_action,
        action_trigger_size=2,
    )

    # assert
    expected_calls = [call([(1, 1 + 2j), (1, 3 + 1j)])]
    assert result_action.call_args_list == expected_calls


def test_external_merge_join(tmp_path: Path):
    # arrange
    left_file = tmp_path / "left.bin"
    right_file = tmp_path / "right.bin"
    left_input_data: list[Row] = [
        {"id": "1", "val": 1},
        {"id": "2", "val": 2},
        {"id": "duplicate", "val": 4},
        {"id": "duplicate", "val": 5},
        {"id": "duplicate_right", "val": 6},
        {"id": "no_join_partner", "val": 0},
    ]
    right_input_data: list[Row] = [
        {"id": "1", "other": 2},
        {"id": "2", "other": 3},
        {"id": "duplicate", "other": 6},
        {"id": "duplicate_right", "other": 7},
        {"id": "duplicate_right", "other": 8},
    ]
    BlockFile(left_file, block_size=SORT_BLOCK_SIZE).write_rows(left_input_data)
    BlockFile(right_file, block_size=SORT_BLOCK_SIZE).write_rows(right_input_data)
    key = lambda row: row["id"]  # noqa: E731

    # act
    joined_data = list(external_merge_join(left_file, right_file, key, key, "inner"))

    # assert
    expected_joined_data = [
        {
            "id": "1",
            "val": 1,
            "other": 2,
        },
        {
            "id": "2",
            "val": 2,
            "other": 3,
        },
        {
            "id": "duplicate",
            "val": 4,
            "other": 6,
        },
        {
            "id": "duplicate",
            "val": 5,
            "other": 6,
        },
        {
            "id": "duplicate_right",
            "val": 6,
            "other": 7,
        },
        {
            "id": "duplicate_right",
            "val": 6,
            "other": 8,
        },
    ]
    assert joined_data == expected_joined_data
