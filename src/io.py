from io import BufferedWriter, BufferedReader
from pathlib import Path
from typing import Any, Iterable
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
    def binarize_block(column_data: list[bytearray], rows) -> bytes:
        assert rows < 255
        block_data = [rows & 0xFF]
        for column in column_data:
            block_data.extend(column)
        return bytes(block_data)

    column_data: list[bytearray] = [bytearray() for _ in header]
    i = 0
    current_block_size = 0
    rows = 0
    while i < len(data):
        if (
            current_block_size > constants.BLOCK_SIZE
            or rows >= constants.MAX_ROWS_PER_BLOCK
        ):
            yield binarize_block(column_data, rows)
            column_data = [bytearray() for _ in header]
            rows = 0
        for j, value in enumerate(data[i]):
            binary_value = binarize_value(value)
            current_block_size += len(binary_value)
            column_data[j].extend(binary_value)
        i += 1
        rows += 1
    if any(d for d in column_data):
        yield binarize_block(column_data, rows)


def serialize(schema, data, output_file: Path):
    with output_file.open(mode="wb") as f:
        serialize_schema(schema, f)
        block_sizes = []
        for data_block in generate_data_blocks(schema, data):
            block_sizes.append(f.tell())
            f.write(data_block)
        for block_size in block_sizes:
            f.write(binarize_value(block_size))
        print("Block sizes:", len(block_sizes))
        f.write(binarize_value(len(block_sizes)))


def deserialize_schema(f: BufferedReader) -> Schema:
    schema_len = int(f.read(1)[0])
    schema = []
    for _ in range(schema_len):
        col_type = ColumnType.from_ordinal(int(f.read(1)[0]))
        name_len = int(f.read(1)[0])
        name = f.read(name_len).decode("utf-8")
        schema.append((name, col_type))
    return schema


def deserialize_block_starts(f: BufferedReader):
    f.seek(-4, os.SEEK_END)
    block_counts = int.from_bytes(f.read(4), byteorder="big", signed=True)
    block_sizes = []
    f.seek(-4 * block_counts - 4, os.SEEK_CUR)
    for _ in range(block_counts):
        block_sizes.append(int.from_bytes(f.read(4), byteorder="big", signed=True))
    return block_sizes


def deserialize_block(f: BufferedReader, header) -> list[list[Any]]:
    block_rows = int(f.read(1)[0])
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


if __name__ == "__main__":
    schema = [
        ("col_a", ColumnType.INTEGER),
        ("col_b", ColumnType.INTEGER),
        ("col_c", ColumnType.STRING),
    ]
    data = [(i, i, f"text{i}") for i in range(100)]
    validate(schema, data)
    serialize(schema, data, Path("data.bin"))
