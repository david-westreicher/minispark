from mini_spark.algorithms import SORT_BLOCK_SIZE, external_sort, kway_merge
from mini_spark.io import BlockFile
from mini_spark.constants import Row
from pathlib import Path
from unittest.mock import Mock, call
from random import shuffle
import random


# TODO: parametrize on col_0, col_1 (sorted string or ints)
def test_external_sort(tmp_path: Path):
    # arrange
    input_file = tmp_path / "input.bin"
    tmp_file = tmp_path / "temp.bin"
    output_file = tmp_path / "sorted.bin"
    random.seed(42)
    input_data: list[Row] = [
        {"int_col": i, "str_col": f"text_{i}"} for i in range(1_000)
    ]
    shuffle(input_data)
    BlockFile(input_file, block_size=SORT_BLOCK_SIZE).write_data_rows(input_data)
    key_function = lambda row: row["int_col"]  # noqa: E731

    # act
    external_sort(input_file, key_function, output_file, tmp_file)
    externally_sorted_data = list(BlockFile(output_file).read_data_rows())

    # assert
    sorted_data = sorted(input_data, key=key_function)
    assert externally_sorted_data == sorted_data


def test_kway_merge_action_trigger_1():
    # arrange
    test_data = [[9, 2, 5, 8], [2, 6, 8, 2, 1], [0, -2, 4]]
    iterators = [iter(list(sorted(data))) for data in test_data]
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
    iterators = [iter(list(sorted(data))) for data in test_data]
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
        iterators, key=lambda x: x[0], action=result_action, action_trigger_size=2
    )

    # assert
    expected_calls = [call([(1, 1 + 2j), (1, 3 + 1j)])]
    assert result_action.call_args_list == expected_calls
