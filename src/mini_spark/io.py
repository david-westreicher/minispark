from __future__ import annotations

import os
from contextlib import ExitStack
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO, Self

from mini_spark.utils import trace

from . import constants
from .constants import Columns, ColumnType, Row, Schema

if TYPE_CHECKING:
    from collections.abc import Iterable
    from io import BufferedWriter
    from pathlib import Path

MAX_COLUMNS = 0xFF
MAX_STR_LENGTH = 0xFF


def validate(schema: Schema, data: list[Row]) -> None:
    for row in data:
        assert len(row) == len(schema)
        for (_, col_type), value in zip(schema, row, strict=True):
            assert col_type.type is type(value)


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
        bytes_to_write.append(len(name) & 0xFF)
        bytes_to_write.extend(name.encode("utf-8"))
    f.write(bytes(bytes_to_write))


def _deserialize_schema(f: BinaryIO) -> Schema:
    schema_len = int(f.read(1)[0])
    schema = []
    for _ in range(schema_len):
        col_type = ColumnType.from_ordinal(int(f.read(1)[0]))
        name_len = int(f.read(1)[0])
        name = f.read(name_len).decode("utf-8")
        schema.append((name, col_type))
    return schema


def _generate_data_blocks_for_columns(
    schema: Schema,
    columns: Columns,
) -> Iterable[bytes]:
    if len(columns) == 0 or len(columns[0]) == 0:
        return
    totalrows = len(columns[0])
    for offset in range(0, totalrows, constants.ROWS_PER_BLOCK):
        block_rows = min(offset + constants.ROWS_PER_BLOCK, totalrows) - offset
        block_bytes = list(to_unsigned_int(block_rows))
        for col, (_, col_type) in zip(columns, schema, strict=True):
            block_data = col[offset : offset + constants.ROWS_PER_BLOCK]
            col_data = []
            if col_type == ColumnType.INTEGER:
                for val in block_data:
                    col_data.extend(val.to_bytes(4, byteorder="little", signed=True))
            elif col_type == ColumnType.STRING:
                col_data.extend(len(val) & 0xFF for val in block_data)
                for val in block_data:
                    col_data.extend(val.encode("utf-8"))
            block_bytes.extend(to_unsigned_int(len(col_data)))
            block_bytes.extend(col_data)
        yield bytes(block_bytes)


def _deserialize_block_column(
    f: BinaryIO,
    column_name: str,
    schema: Schema,
    block_rows: int,
    chunk_size: float = float("inf"),  # TODO(david): not needed anymore
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
        str_lengths = [int(f.read(1)[0]) for _ in range(block_rows)]
        while rows_read < block_rows:
            str_value = f.read(str_lengths[rows_read]).decode("utf-8")
            curr_chunk.append(str_value)
            rows_read += 1
            if len(curr_chunk) > chunk_size:
                yield curr_chunk
                curr_chunk = []
    else:
        raise ValueError(f"Unsupported column type {col_type}")
    if curr_chunk:
        yield curr_chunk


@trace("deserialize block")
def _deserialize_block(f: BinaryIO, schema: Schema) -> Columns:
    block_rows = read_unsigned_int(f)
    data: list[list[Any]] = [[] for _ in schema]
    block_data_start = f.tell()
    for i, (col_name, _) in enumerate(schema):
        data[i] = [row for chunk in _deserialize_block_column(f, col_name, schema, block_rows) for row in chunk]
        f.seek(block_data_start)
    return tuple(data)


def _deserialize_block_starts(f: BinaryIO) -> list[int]:
    f.seek(-4, os.SEEK_END)
    block_counts = read_unsigned_int(f)
    f.seek(-4 * block_counts - 4, os.SEEK_CUR)
    return [read_unsigned_int(f) for _ in range(block_counts)]


@dataclass
class BlockFile:
    file: Path
    schema: Schema = field(default_factory=list)

    @cached_property
    def block_starts(self) -> list[int]:
        with self.file.open("rb") as f:
            return _deserialize_block_starts(f)

    @cached_property
    def file_schema(self) -> Schema:
        with self.file.open("rb") as f:
            return _deserialize_schema(f)

    def write_data(self, data: Columns, schema: Schema | None = None) -> Self:
        if not schema:
            schema = self.schema
        return self._write_data_with_known_schema(data, schema)

    def write_rows(self, data: list[Row]) -> Self:
        if len(data) == 0:
            if self.schema:
                with self.file.open(mode="wb") as f:
                    _serialize_schema(self.schema, f)
            return self
        first_row = data[0]
        self.schema = [(key, ColumnType.of(value)) for key, value in first_row.items()]
        columns_data = tuple([row[col_name] for row in data] for (col_name, _) in self.schema)
        return self._write_data_with_known_schema(columns_data, self.schema)

    def _write_data_with_known_schema(self, columns_data: Columns, schema: Schema) -> Self:
        assert schema
        with self.file.open(mode="wb") as f:
            _serialize_schema(schema, f)
            block_starts = []
            for data_block in _generate_data_blocks_for_columns(schema, columns_data):
                block_starts.append(f.tell())
                f.write(data_block)
            for block_size in block_starts:
                f.write(to_unsigned_int(block_size))
            f.write(to_unsigned_int(len(block_starts)))
        return self

    def append_data(self, data: Columns) -> Self:
        if not self.file.exists():
            return self.write_data(data)
        # TODO(david): Should append to last block, so that rows per block stays constant (except last block)
        schema = self.file_schema
        assert schema == self.schema
        block_starts = self.block_starts
        with self.file.open(mode="rb+") as f:
            f.seek(-4 * (len(block_starts) + 1), os.SEEK_END)
            for data_block in _generate_data_blocks_for_columns(schema, data):
                block_starts.append(f.tell())
                f.write(data_block)
            for block_size in block_starts:
                f.write(to_unsigned_int(block_size))
            f.write(to_unsigned_int(len(block_starts)))
        return self

    def append_tuples(self, data: list[tuple[Any, ...]]) -> Self:
        columns_data = tuple(map(list, zip(*data, strict=True)))
        return self.append_data(columns_data)

    def append_rows(self, data: list[Row]) -> Self:
        assert self.schema
        first_row = data[0]
        schema = [(key, ColumnType.of(value)) for key, value in first_row.items()]
        columns_data = tuple([row[col_name] for row in data] for (col_name, _) in schema)
        return self.append_data(columns_data)

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

    def read_block_data_columns_sequentially(self) -> Iterable[Columns]:
        schema = self.file_schema
        with self.file.open("rb") as f:
            for block_start in self.block_starts:
                f.seek(block_start)
                yield _deserialize_block(f, schema)

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
        self.schema = BlockFile(files[0]).file_schema
        assert all(BlockFile(f).file_schema == self.schema for f in files)
        for file in files:
            block_file = BlockFile(file)
            for i in range(len(block_file.block_starts)):
                block_data = block_file.read_block_data_columns_by_id(i)
                self.append_data(block_data)
        return self
