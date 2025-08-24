from pathlib import Path
from mini_spark.io import ColumnType, serialize, deserialize_schema, deserialize, append


def test_serialize_deserialize_schema(temporary_file: Path):
    # arrange
    schema = [
        ("int_col", ColumnType.INTEGER),
        ("str_col", ColumnType.STRING),
    ]

    # act
    serialize(schema, [], temporary_file)
    deserialized_schema = deserialize_schema(temporary_file.open("rb"))

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
    serialize(schema, data, temporary_file)
    deserialized_schema, all_data = deserialize(temporary_file)

    # assert
    assert deserialized_schema == schema
    assert all_data == [[1, 2], ["1", "2"]]


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
    serialize(schema, data, temporary_file)
    append(new_data, temporary_file)
    deserialized_schema, all_data = deserialize(temporary_file)

    # assert
    assert deserialized_schema == schema
    assert all_data == [[1, 2, 3, 4], ["1", "2", "3", "4"]]
