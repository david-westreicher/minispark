from pathlib import Path
from mini_spark.io import (
    ColumnType,
    _deserialize_schema,
    BlockFile,
)


def test_serialize_deserialize_schema(temporary_file: Path):
    # arrange
    schema = [
        ("int_col", ColumnType.INTEGER),
        ("str_col", ColumnType.STRING),
    ]

    # act
    block_file = BlockFile(temporary_file)
    block_file.write_data([], schema)
    deserialized_schema = _deserialize_schema(temporary_file.open("rb"))

    # assert
    assert deserialized_schema == schema


def test_serialize_deserialize_data(temporary_file: Path):
    # arrange
    schema = [
        ("int_col", ColumnType.INTEGER),
        ("str_col", ColumnType.STRING),
    ]
    data = [
        (1, "1"),
        (2, "2"),
    ]

    # act
    block_file = BlockFile(temporary_file)
    block_file.write_data(data, schema)
    deserialized_schema = block_file.file_schema
    all_data = list(block_file.read_data())

    # assert
    assert deserialized_schema == schema
    assert all_data == data


def test_serialize_append_deserialize_data(temporary_file: Path):
    # arrange
    schema = [
        ("int_col", ColumnType.INTEGER),
        ("str_col", ColumnType.STRING),
    ]
    data = [
        (1, "1"),
        (2, "2"),
    ]
    new_data = [
        (3, "3"),
        (4, "4"),
    ]

    # act
    block_file = BlockFile(temporary_file)
    block_file.write_data(data, schema)
    block_file.append_data(new_data)
    deserialized_schema = block_file.file_schema
    all_data = list(block_file.read_data())

    # assert
    assert deserialized_schema == schema
    assert all_data == data + new_data
