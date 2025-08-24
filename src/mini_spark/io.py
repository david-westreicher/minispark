from io import BufferedWriter
from pathlib import Path
from typing import Any, BinaryIO, Iterable
import os
from . import constants
from enum import Enum


class ColumnType(Enum):
    INTEGER = (0, int)
    STRING = (1, str)
    UNKNOWN = (255, type(None))

    def __init__(self, value, py_type):
        self.ordinal = value
        self.type = py_type

    @staticmethod
    def from_ordinal(ordinal: int):
        for type in ColumnType:
            if type.ordinal == ordinal:
                return type

    @staticmethod
    def of(value: Any):
        if type(value) is int:
            return ColumnType.INTEGER
        elif type(value) is str:
            return ColumnType.STRING
        else:
            return ColumnType.UNKNOWN


Schema = list[tuple[str, ColumnType]]


def validate(schema, data):
    for row in data:
        assert len(row) == len(schema)
        for (_, col_type), value in zip(schema, row):
            assert col_type.type is type(value)


def binarize_value(value) -> bytes:
    if type(value) is int:
        return value.to_bytes(4, byteorder="big", signed=True)
    elif type(value) is str:
        assert len(value) < 255
        return bytes([len(value) & 0xFF] + list(value.encode("utf-8")))
    raise ValueError(f"Unsupported type {type(value)}:{value}")


def to_unsigned_int(value: int) -> bytes:
    return value.to_bytes(4, byteorder="big", signed=False)


def read_unsigned_int(f: BinaryIO) -> int:
    return int.from_bytes(f.read(4), byteorder="big", signed=False)


def serialize_schema(schema: Schema, f: BufferedWriter):
    # | schema-len |
    # ---- repeat 'schema-len' times ----
    # | column type |
    # | name-len |
    # | name in unicode |
    # ---- repeat ends -----
    assert len(schema) < 255
    bytes_to_write = [len(schema) & 0xFF]
    for name, type in schema:
        assert len(name) < 255
        bytes_to_write.append(type.ordinal & 0xFF)
        bytes_to_write.extend(binarize_value(name))
    f.write(bytes(bytes_to_write))


def generate_data_blocks(header, data) -> Iterable[bytes]:
    def binarize_block(column_data: list[bytearray], row_count: int) -> bytes:
        block_data: list[int] = []
        block_data.extend(to_unsigned_int(row_count))
        for column in column_data:
            block_data.extend(column)
        return bytes(block_data)

    column_data: list[bytearray] = [bytearray() for _ in header]
    i = 0
    current_block_size = 0
    rows = 0
    while i < len(data):
        if current_block_size > constants.BLOCK_SIZE:
            yield binarize_block(column_data, rows)
            column_data = [bytearray() for _ in header]
            current_block_size = 0
            rows = 0
        assert len(data[i]) == len(header)
        for j, value in enumerate(data[i]):
            binary_value = binarize_value(value)
            current_block_size += len(binary_value)
            column_data[j].extend(binary_value)
        i += 1
        rows += 1
    if any(d for d in column_data):
        yield binarize_block(column_data, rows)


def serialize(schema: Schema, data: list[tuple[Any, ...]], output_file: Path):
    with output_file.open(mode="wb") as f:
        serialize_schema(schema, f)
        block_starts = []
        for data_block in generate_data_blocks(schema, data):
            block_starts.append(f.tell())
            f.write(data_block)
        for block_size in block_starts:
            f.write(binarize_value(block_size))
        print("Serialize, block numbers:", len(block_starts))
        f.write(to_unsigned_int(len(block_starts)))


def append(data: list[tuple[Any, ...]], output_file: Path):
    with output_file.open(mode="rb+") as f:
        schema = deserialize_schema(f)
        block_starts = deserialize_block_starts(f)
        f.seek(-4 * (len(block_starts) + 1), os.SEEK_END)
        for data_block in generate_data_blocks(schema, data):
            block_starts.append(f.tell())
            f.write(data_block)
        for block_size in block_starts:
            f.write(binarize_value(block_size))
        print("Serialize, block numbers:", len(block_starts))
        f.write(to_unsigned_int(len(block_starts)))


def append_rows(data: list[dict[str, Any]], output_file: Path):
    if not output_file.exists():
        serialize_rows(data, output_file)
        return
    with output_file.open(mode="rb+") as f:
        schema = deserialize_schema(f)
    data_tuples = [tuple(row[col_name] for col_name, _ in schema) for row in data]
    append(data_tuples, output_file)


def serialize_rows(data: list[dict[str, Any]], output_file: Path):
    first_row = data[0]
    schema = [(col_name, ColumnType.of(value)) for col_name, value in first_row.items()]
    data_tuples = [tuple(row[col_name] for col_name, _ in schema) for row in data]
    serialize(schema, data_tuples, output_file)


def deserialize_schema(f: BinaryIO) -> Schema:
    schema_len = int(f.read(1)[0])
    schema = []
    for _ in range(schema_len):
        col_type = ColumnType.from_ordinal(int(f.read(1)[0]))
        name_len = int(f.read(1)[0])
        name = f.read(name_len).decode("utf-8")
        schema.append((name, col_type))
    return schema


def deserialize_block_starts(f: BinaryIO) -> list[int]:
    f.seek(-4, os.SEEK_END)
    block_counts = read_unsigned_int(f)
    block_sizes = []
    f.seek(-4 * block_counts - 4, os.SEEK_CUR)
    for _ in range(block_counts):
        block_sizes.append(int.from_bytes(f.read(4), byteorder="big", signed=True))
    return block_sizes


def deserialize_block(f: BinaryIO, header) -> list[list[Any]]:
    block_rows = read_unsigned_int(f)
    data: list[list[Any]] = [[] for _ in header]
    for i, (col_name, col_type) in enumerate(header):
        col_data: list[Any] = []
        if col_type == ColumnType.INTEGER:
            col_data = [
                int.from_bytes(f.read(4), byteorder="big", signed=True)
                for _ in range(block_rows)
            ]
        elif col_type == ColumnType.STRING:
            col_data = []
            for _ in range(block_rows):
                str_len = int(f.read(1)[0])
                str_value = f.read(str_len).decode("utf-8")
                col_data.append(str_value)
        data[i] = col_data
    return data


def deserialize(input_file: Path) -> tuple[Schema, list[list[Any]]]:
    with input_file.open("rb") as f:
        schema = deserialize_schema(f)
        block_starts = deserialize_block_starts(f)
        print("Deserialize: block sizes:", len(block_starts))
        all_data: list[list[Any]] = [[] for _ in schema]
        for block_start in block_starts:
            f.seek(block_start)
            block_data = deserialize_block(f, schema)
            for i, col_data in enumerate(block_data):
                all_data[i].extend(col_data)
        return schema, all_data


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10)
    args = parser.parse_args()

    schema = [
        ("col_a", ColumnType.INTEGER),
        ("col_b", ColumnType.INTEGER),
        ("col_c", ColumnType.STRING),
    ]
    data = [(i, i, f"text{i % 2}") for i in range(args.rows)]
    validate(schema, data)
    serialize(schema, data, Path("data.bin"))
