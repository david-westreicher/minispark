from io import BufferedWriter
from pathlib import Path
from functools import cached_property
from typing import Any, BinaryIO, Iterable, Self
from dataclasses import dataclass, field
import os
from contextlib import ExitStack
from . import constants
from .constants import Row
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


def generate_data_blocks(
    schema: Schema,
    data: Iterable[tuple[Any, ...]],
    max_block_size: int = constants.BLOCK_SIZE,
) -> Iterable[bytes]:
    def binarize_block(column_data: list[bytearray], row_count: int) -> bytes:
        block_data: list[int] = []
        block_data.extend(to_unsigned_int(row_count))
        for column in column_data:
            block_data.extend(to_unsigned_int(len(column)))
            block_data.extend(column)
        return bytes(block_data)

    column_data: list[bytearray] = [bytearray() for _ in schema]
    current_block_size = 0
    rows = 0
    for row in data:
        if current_block_size > max_block_size:
            yield binarize_block(column_data, rows)
            column_data = [bytearray() for _ in schema]
            current_block_size = 0
            rows = 0
        assert len(row) == len(schema)
        for j, value in enumerate(row):
            binary_value = binarize_value(value)
            current_block_size += len(binary_value)
            column_data[j].extend(binary_value)
        rows += 1
    if any(d for d in column_data):
        yield binarize_block(column_data, rows)


def serialize(
    schema: Schema,
    data: Iterable[tuple[Any, ...]],
    output_file: Path,
    block_size: int = constants.BLOCK_SIZE,
):
    with output_file.open(mode="wb") as f:
        serialize_schema(schema, f)
        block_starts = []
        for data_block in generate_data_blocks(schema, data, block_size):
            block_starts.append(f.tell())
            f.write(data_block)
        for block_size in block_starts:
            f.write(binarize_value(block_size))
        print("Serialize, block numbers:", len(block_starts), output_file)
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
        print("Append, block numbers:", len(block_starts), output_file)
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


def deserialize_block_column(
    f: BinaryIO,
    column_name: str,
    schema: Schema,
    block_rows: int,
    chunk_size: int = float("inf"),
) -> Iterable[list[Any]]:
    # skip to correct column
    for schema_col_name, _ in schema:
        column_data_size = read_unsigned_int(f)
        if schema_col_name != column_name:
            f.seek(column_data_size, os.SEEK_CUR)
        else:
            break

    col_type = next(
        col_type for col_name, col_type in schema if col_name == column_name
    )
    rows_read = 0
    curr_chunk: list[Any] = []
    if col_type == ColumnType.INTEGER:
        while rows_read < block_rows:
            int_value = int.from_bytes(f.read(4), byteorder="big", signed=True)
            curr_chunk.append(int_value)
            rows_read += 1
            if len(curr_chunk) > chunk_size:
                yield curr_chunk
                curr_chunk = []
    elif col_type == ColumnType.STRING:
        while rows_read < block_rows:
            str_len = int(f.read(1)[0])
            str_value = f.read(str_len).decode("utf-8")
            curr_chunk.append(str_value)
            rows_read += 1
            if len(curr_chunk) > chunk_size:
                yield curr_chunk
                curr_chunk = []
    else:
        raise ValueError(f"Unsupported column type {col_type}")
    if curr_chunk:
        yield curr_chunk


def deserialize_block(
    f: BinaryIO, schema, offset: int = -1, limit: int = -1
) -> list[list[Any]]:
    block_rows = read_unsigned_int(f)
    data: list[list[Any]] = [[] for _ in schema]
    block_data_start = f.tell()
    for i, (col_name, col_type) in enumerate(schema):
        data[i] = list(
            row
            for chunk in deserialize_block_column(f, col_name, schema, block_rows)
            for row in chunk
        )
        f.seek(block_data_start)
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


@dataclass
class BlockFile:
    file: Path
    block_size: int = constants.BLOCK_SIZE
    schema: Schema = field(default_factory=list)

    def write_data(self, data: list[tuple[Any, ...]], schema: Schema = []) -> Self:
        first_row = data[0]
        if not schema:
            schema = [
                (f"col_{i}", ColumnType.of(value)) for i, value in enumerate(first_row)
            ]
        return self.write_data_with_known_schema(data, schema)

    def write_data_with_known_schema(
        self, data: Iterable[tuple[Any, ...]], schema: Schema = []
    ) -> Self:
        assert schema
        serialize(schema, data, self.file, block_size=self.block_size)
        return self

    def append_rows(self, data: list[Row], schema: Schema = []) -> Self:
        if not self.file.exists():
            self.write_data_rows(data)
            return self
        schema = self.file_schema
        data_tuples = [tuple(row[col_name] for col_name, _ in schema) for row in data]
        append(data_tuples, self.file)
        return self

    def create_block_reader(
        self, block_start: int, row_buffer_size: int
    ) -> Iterable[Row]:
        schema = self.file_schema
        with self.file.open("rb") as f:
            f.seek(block_start)
            block_rows = read_unsigned_int(f)
        with ExitStack() as stack:
            file_handles = [stack.enter_context(self.file.open("rb")) for _ in schema]
            for f in file_handles:
                f.seek(block_start)
                block_rows = read_unsigned_int(f)
            column_readers = [
                deserialize_block_column(
                    f, col_name, schema, block_rows, chunk_size=row_buffer_size
                )
                for f, (col_name, _) in zip(file_handles, schema)
            ]
            for row_tuple_chunk in zip(*column_readers):
                yield from [
                    {
                        col_name: row_tuple[col_idx]
                        for col_idx, (col_name, _) in enumerate(schema)
                    }
                    for row_tuple in zip(*row_tuple_chunk)
                ]

    @cached_property
    def block_starts(self) -> list[int]:
        with self.file.open("rb") as f:
            return deserialize_block_starts(f)

    @cached_property
    def file_schema(self) -> Schema:
        with self.file.open("rb") as f:
            return deserialize_schema(f)

    def read_blocks_sequentially(self) -> Iterable[list[Row]]:
        schema = self.file_schema
        block_starts = self.block_starts
        with self.file.open("rb") as f:
            for block_start in block_starts:
                f.seek(block_start)
                block_data = deserialize_block(f, schema)
                row_data = [
                    {
                        col_name: block_data[col_idx][row_idx]
                        for col_idx, (col_name, _) in enumerate(schema)
                    }
                    for row_idx in range(len(block_data[0]))
                ]
                yield row_data

    def write_data_rows(self, data: list[Row]) -> Self:
        first_row = data[0]
        schema = [(key, ColumnType.of(value)) for key, value in first_row.items()]
        tuple_data = [tuple(row[key] for key, _ in schema) for row in data]
        return self.write_data(tuple_data, schema=schema)

    def read_data_rows(self) -> Iterable[Row]:
        for block in self.read_blocks_sequentially():
            yield from block

    def read_data(self) -> Iterable[tuple[Any, ...]]:
        schema = self.file_schema
        block_starts = self.block_starts
        with self.file.open("rb") as f:
            for block_start in block_starts:
                f.seek(block_start)
                block_data = deserialize_block(f, schema)
                yield from zip(*block_data)

    def merge_files(self, files: list[Path]) -> Self:
        schema = BlockFile(files[0]).file_schema
        assert all(BlockFile(f).file_schema == schema for f in files)
        full_data_iterator = (row for f in files for row in BlockFile(f).read_data())
        return self.write_data_with_known_schema(full_data_iterator, schema)
