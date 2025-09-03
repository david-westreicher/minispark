from pathlib import Path
from unittest.mock import patch

from mini_spark.constants import ColumnType
from mini_spark.io import (
    BlockFile,
    _deserialize_schema,
)


def test_serialize_deserialize_schema(temporary_file: Path):
    # arrange
    schema = [
        ("int_col", ColumnType.INTEGER),
        ("str_col", ColumnType.STRING),
    ]

    # act
    block_file = BlockFile(temporary_file, schema)
    block_file.write_rows([])
    deserialized_schema = _deserialize_schema(temporary_file.open("rb"))

    # assert
    assert deserialized_schema == schema


def test_serialize_deserialize_data(temporary_file: Path):
    # arrange
    rows = [
        {"int_col": 1, "str_col": "1"},
        {"int_col": 2, "str_col": "2"},
    ]

    # act
    block_file = BlockFile(temporary_file)
    block_file.write_rows(rows)
    deserialized_schema = block_file.file_schema
    all_data = list(block_file.read_data_rows())

    # assert
    assert deserialized_schema == block_file.schema
    assert all_data == rows


def test_serialize_append_deserialize_data(temporary_file: Path):
    # arrange
    rows = [
        {"int_col": 1, "str_col": "1"},
        {"int_col": 2, "str_col": "2"},
    ]
    new_rows = [
        {"int_col": 3, "str_col": "3"},
        {"int_col": 4, "str_col": "4"},
    ]

    # act
    block_file = BlockFile(temporary_file)
    block_file.write_rows(rows)
    block_file.append_rows(new_rows)
    deserialized_schema = block_file.file_schema
    all_data = list(block_file.read_data_rows())

    # assert
    assert deserialized_schema == [("int_col", ColumnType.INTEGER), ("str_col", ColumnType.STRING)]
    assert all_data == rows + new_rows


def test_append_keeping_max_row_size(temporary_file: Path):
    # arrange
    block_file = BlockFile(temporary_file, [("col1", ColumnType.STRING)]).write_rows([])

    # act/assert
    with patch("mini_spark.io.ROWS_PER_BLOCK", 10):
        for _ in range(10):
            block_file.append_rows([{"col1": "x"}])
            assert len(BlockFile(temporary_file).block_starts) == 1

        # this triggers the creation of a new block
        block_file.append_rows([{"col1": "x"}])
        assert len(BlockFile(temporary_file).block_starts) == 2
        assert len(list(BlockFile(temporary_file).read_data_rows())) == 11

        # now we add 5 -> still same block
        block_file.append_rows([{"col1": "x"}] * 5)
        assert len(BlockFile(temporary_file).block_starts) == 2
        assert len(list(BlockFile(temporary_file).read_data_rows())) == 16

        # now we add 5 -> new block
        block_file.append_rows([{"col1": "x"}] * 5)
        assert len(BlockFile(temporary_file).block_starts) == 3
        assert len(list(BlockFile(temporary_file).read_data_rows())) == 21
