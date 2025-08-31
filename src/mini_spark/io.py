from __future__ import annotations

import os
from contextlib import ExitStack
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO, Self

from . import constants
from .constants import Columns, ColumnType, Row, Schema
from .utils import convert_columns_to_rows

if TYPE_CHECKING:
    from collections.abc import Iterable
    from io import BufferedWriter
    from pathlib import Path

MAX_COLUMNS = 255
MAX_STR_LENGTH = 255


def validate(schema: Schema, data: list[Row]) -> None:
    for row in data:
        assert len(row) == len(schema)
        for (_, col_type), value in zip(schema, row, strict=True):
            assert col_type.type is type(value)


def binarize_value(value: int | str) -> bytes:
    if type(value) is int:
        return value.to_bytes(4, byteorder="little", signed=True)
    if type(value) is str:
        assert len(value) < MAX_STR_LENGTH
        return bytes([len(value) & 255, *list(value.encode("utf-8"))])
    raise ValueError(f"Unsupported type {type(value)}:{value}")


def to_unsigned_int(value: int) -> bytes:
    return value.to_bytes(4, byteorder="little", signed=False)


def read_unsigned_int(f: BinaryIO) -> int:
    return int.from_bytes(f.read(4), byteorder="little", signed=False)


def _serialize_schema(schema: Schema, f: BufferedWriter) -> None:
    # | schema-len |
    # ---- repeat 'schema-len' times ----
    # | column type |
    # | name-len |
    # | name in unicode |
    # ---- repeat ends -----
    assert len(schema) < MAX_COLUMNS
    bytes_to_write = [len(schema) & 0xFF]
    for name, col_type in schema:
        assert len(name) < MAX_STR_LENGTH
        bytes_to_write.append(col_type.ordinal & 0xFF)
        bytes_to_write.extend(binarize_value(name))
    f.write(bytes(bytes_to_write))


def _generate_data_blocks(
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


def _deserialize_schema(f: BinaryIO) -> Schema:
    schema_len = int(f.read(1)[0])
    schema = []
    for _ in range(schema_len):
        col_type = ColumnType.from_ordinal(int(f.read(1)[0]))
        name_len = int(f.read(1)[0])
        name = f.read(name_len).decode("utf-8")
        schema.append((name, col_type))
    return schema


def _deserialize_block_starts(f: BinaryIO) -> list[int]:
    f.seek(-4, os.SEEK_END)
    block_counts = read_unsigned_int(f)
    f.seek(-4 * block_counts - 4, os.SEEK_CUR)
    return [read_unsigned_int(f) for _ in range(block_counts)]


def _deserialize_block_column(
    f: BinaryIO,
    column_name: str,
    schema: Schema,
    block_rows: int,
    chunk_size: float = float("inf"),
) -> Iterable[list[Any]]:
    # skip to correct column
    for schema_col_name, _ in schema:
        column_data_size = read_unsigned_int(f)
        if schema_col_name != column_name:
            f.seek(column_data_size, os.SEEK_CUR)
        else:
            break

    col_type = next(col_type for col_name, col_type in schema if col_name == column_name)
    rows_read = 0
    curr_chunk: list[Any] = []
    if col_type == ColumnType.INTEGER:
        while rows_read < block_rows:
            int_value = int.from_bytes(f.read(4), byteorder="little", signed=True)
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


def _deserialize_block(f: BinaryIO, schema: Schema) -> Columns:
    block_rows = read_unsigned_int(f)
    data: list[list[Any]] = [[] for _ in schema]
    block_data_start = f.tell()
    for i, (col_name, _) in enumerate(schema):
        data[i] = [row for chunk in _deserialize_block_column(f, col_name, schema, block_rows) for row in chunk]
        f.seek(block_data_start)
    return tuple(data)


@dataclass
class BlockFile:
    file: Path
    block_size: int = constants.BLOCK_SIZE
    schema: Schema = field(default_factory=list)

    @cached_property
    def block_starts(self) -> list[int]:
        with self.file.open("rb") as f:
            return _deserialize_block_starts(f)

    @cached_property
    def file_schema(self) -> Schema:
        with self.file.open("rb") as f:
            return _deserialize_schema(f)

    def write_tuples(self, data: list[tuple[Any, ...]], schema: Schema | None = None) -> Self:
        if not schema:
            first_row = data[0]
            schema = [(f"col_{i}", ColumnType.of(value)) for i, value in enumerate(first_row)]
        return self._write_data_with_known_schema(data, schema)

    def write_rows(self, data: list[Row]) -> Self:
        first_row = data[0]
        schema = [(key, ColumnType.of(value)) for key, value in first_row.items()]
        tuple_data = [tuple(row[key] for key, _ in schema) for row in data]
        return self._write_data_with_known_schema(tuple_data, schema=schema)

    def _write_data_with_known_schema(self, data: Iterable[tuple[Any, ...]], schema: Schema | None = None) -> Self:
        assert schema
        with self.file.open(mode="wb") as f:
            _serialize_schema(schema, f)
            block_starts = []
            for data_block in _generate_data_blocks(schema, data, self.block_size):
                block_starts.append(f.tell())
                f.write(data_block)
            for block_size in block_starts:
                f.write(to_unsigned_int(block_size))
            f.write(to_unsigned_int(len(block_starts)))
        return self

    def append_data(self, data: Columns) -> Self:
        assert self.schema
        rows = list(convert_columns_to_rows(data, self.schema))
        return self.append_rows(rows)

    def append_tuples(self, data: list[tuple[Any, ...]], schema: Schema | None = None) -> Self:
        if not self.file.exists():
            self.write_tuples(data, schema)
            return self
        schema = self.file_schema
        block_starts = self.block_starts
        with self.file.open(mode="rb+") as f:
            f.seek(-4 * (len(block_starts) + 1), os.SEEK_END)
            for data_block in _generate_data_blocks(schema, data):
                block_starts.append(f.tell())
                f.write(data_block)
            for block_size in block_starts:
                f.write(to_unsigned_int(block_size))
            f.write(to_unsigned_int(len(block_starts)))
        return self

    def append_rows(self, data: list[Row]) -> Self:
        if not self.file.exists():
            self.write_rows(data)
            return self
        schema = self.file_schema
        data_tuples = [tuple(row[col_name] for col_name, _ in schema) for row in data]
        return self.append_tuples(data_tuples)

    def read_data(self) -> Iterable[tuple[Any, ...]]:
        schema = self.file_schema
        block_starts = self.block_starts
        with self.file.open("rb") as f:
            for block_start in block_starts:
                f.seek(block_start)
                block_data = _deserialize_block(f, schema)
                yield from zip(*block_data, strict=True)

    def read_data_rows(self) -> Iterable[Row]:
        for block in self.read_blocks_sequentially():
            yield from block

    def read_block_data(self, block_id: int) -> list[tuple[Any, ...]]:
        block_data_columns = self.read_block_data_columns_by_id(block_id)
        return list(zip(*block_data_columns, strict=True))

    def read_block_data_columns_by_id(self, block_id: int) -> Columns:
        schema = self.file_schema
        block_start = self.block_starts[block_id]
        with self.file.open("rb") as f:
            f.seek(block_start)
            return _deserialize_block(f, schema)

    def read_block_rows(self, block_id: int) -> Iterable[Row]:
        schema = self.file_schema
        block_start = self.block_starts[block_id]
        for row in self.read_block_data(block_start):
            yield {col_name: row[col_idx] for col_idx, (col_name, _) in enumerate(schema)}

    def read_blocks_sequentially(self) -> Iterable[list[Row]]:
        schema = self.file_schema
        for block_id in range(len(self.block_starts)):
            block_data = self.read_block_data(block_id)
            row_data = [{col_name: val for val, (col_name, _) in zip(row, schema, strict=True)} for row in block_data]
            yield row_data

    def create_block_reader(
        self,
        block_start: int,
        row_buffer_size: int,
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
                _deserialize_block_column(
                    f,
                    col_name,
                    schema,
                    block_rows,
                    chunk_size=row_buffer_size,
                )
                for f, (col_name, _) in zip(file_handles, schema, strict=True)
            ]
            for row_tuple_chunk in zip(*column_readers, strict=True):
                yield from [
                    {col_name: row_tuple[col_idx] for col_idx, (col_name, _) in enumerate(schema)}
                    for row_tuple in zip(*row_tuple_chunk, strict=True)
                ]

    def merge_files(self, files: list[Path]) -> Self:
        schema = BlockFile(files[0]).file_schema
        assert all(BlockFile(f).file_schema == schema for f in files)
        full_data_iterator = (row for f in files for row in BlockFile(f).read_data())
        return self._write_data_with_known_schema(full_data_iterator, schema)
