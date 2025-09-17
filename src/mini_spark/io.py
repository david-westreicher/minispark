from __future__ import annotations

import os
import struct
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO, Self

from .constants import ROWS_PER_BLOCK, Columns, ColumnType, Row, Schema

if TYPE_CHECKING:
    from collections.abc import Iterable
    from io import BufferedRandom, BufferedWriter
    from pathlib import Path

MAX_COLUMNS = 0xFF
MAX_STR_LENGTH = 0xFF
LONG_BYTE_COUNT = 8


def to_unsigned_int(value: int, byte_count: int = 4) -> bytes:
    if byte_count == LONG_BYTE_COUNT:
        return struct.pack("<Q", value)
    return value.to_bytes(4, byteorder="little", signed=False)


def read_unsigned_int(f: BinaryIO, byte_count: int = 4) -> int:
    if byte_count == LONG_BYTE_COUNT:
        return int(struct.unpack("<Q", f.read(LONG_BYTE_COUNT))[0])
    return int.from_bytes(f.read(4), byteorder="little", signed=False)


def datetime_to_timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000)


def timestamp_to_datetime(microseconds_since_epoch: int) -> datetime:
    return datetime.fromtimestamp(microseconds_since_epoch / 1_000_000)


def encode_string(text: str) -> bytes:
    assert len(text) < MAX_STR_LENGTH
    return bytes([len(text) & 0xFF]) + text.encode("utf-8")


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
        bytes_to_write.extend(encode_string(name))
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


def _generate_data_blocks_for_columns(  # noqa: C901
    schema: Schema,
    columns: Columns,
) -> Iterable[bytes]:
    if len(columns) == 0 or len(columns[0]) == 0:
        return
    totalrows = len(columns[0])
    for offset in range(0, totalrows, ROWS_PER_BLOCK):
        block_rows = min(offset + ROWS_PER_BLOCK, totalrows) - offset
        block_bytes = list(to_unsigned_int(block_rows))
        for col, (_, col_type) in zip(columns, schema, strict=True):
            block_data = col[offset : offset + ROWS_PER_BLOCK]
            col_data: list[int] = []
            if col_type == ColumnType.INTEGER:
                for val in block_data:
                    assert type(val) is int
                    col_data.extend(val.to_bytes(4, byteorder="little", signed=True))
            elif col_type == ColumnType.FLOAT:
                for val in block_data:
                    assert type(val) is float
                    col_data.extend(struct.pack("<f", val))
            elif col_type == ColumnType.TIMESTAMP:
                for val in block_data:
                    date_val = datetime.fromisoformat(val) if type(val) is str else val
                    assert type(date_val) is datetime
                    col_data.extend(struct.pack("<q", datetime_to_timestamp(date_val)))
            elif col_type == ColumnType.STRING:
                col_data.extend(len(str(val)) & 0xFF for val in block_data)
                for val in block_data:
                    assert type(val) is str
                    col_data.extend(val.encode("utf-8"))
            else:
                raise ValueError(f"Unsupported column type {col_type}")
            block_bytes.extend(to_unsigned_int(len(col_data), LONG_BYTE_COUNT))
            block_bytes.extend(col_data)
        yield bytes(block_bytes)


def _deserialize_block_column(  # noqa: C901, PLR0912
    f: BinaryIO,
    column_name: str,
    schema: Schema,
    block_rows: int,
) -> Iterable[list[Any]]:
    # skip to correct column
    for schema_col_name, _ in schema:
        column_data_size = read_unsigned_int(f, LONG_BYTE_COUNT)
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
    elif col_type == ColumnType.FLOAT:
        while rows_read < block_rows:
            float_value = struct.unpack("<f", f.read(4))[0]
            curr_chunk.append(float_value)
            rows_read += 1
    elif col_type == ColumnType.TIMESTAMP:
        while rows_read < block_rows:
            int_value = struct.unpack("<q", f.read(LONG_BYTE_COUNT))[0]
            curr_chunk.append(timestamp_to_datetime(int_value))
            rows_read += 1
    elif col_type == ColumnType.STRING:
        str_lengths = [int(f.read(1)[0]) for _ in range(block_rows)]
        while rows_read < block_rows:
            str_value = f.read(str_lengths[rows_read]).decode("utf-8")
            curr_chunk.append(str_value)
            rows_read += 1
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


def _deserialize_block_starts(f: BinaryIO) -> list[int]:
    f.seek(-4, os.SEEK_END)
    block_counts = read_unsigned_int(f)
    f.seek(-LONG_BYTE_COUNT * block_counts - 4, os.SEEK_CUR)
    return [read_unsigned_int(f, LONG_BYTE_COUNT) for _ in range(block_counts)]


def merge_data_blocks(data_block_1: Columns, data_block_2: Columns) -> Columns:
    merged_data = []
    for col1, col2 in zip(data_block_1, data_block_2, strict=True):
        merged_data.append(col1 + col2)
    return tuple(merged_data)


@dataclass
class BlockFile:
    file: Path
    schema: Schema = field(default_factory=list)
    _block_starts: list[int] | None = field(default=None, init=False, repr=False)

    @property
    def block_starts(self) -> list[int]:
        if self._block_starts is None:
            with self.file.open("rb") as f:
                self._block_starts = _deserialize_block_starts(f)
        return self._block_starts

    @cached_property
    def file_schema(self) -> Schema:
        with self.file.open("rb") as f:
            return _deserialize_schema(f)

    def write_data(self, data: Columns) -> Self:
        return self._write_data_with_known_schema(data, self.schema)

    def write_tuples(self, tuples: list[tuple[Any, ...]]) -> Self:
        data = tuple(map(list, zip(*tuples, strict=True)))
        return self._write_data_with_known_schema(data, self.schema)

    def write_rows(self, data: list[Row]) -> Self:
        if len(data) == 0:
            if self.schema:
                with self.file.open(mode="wb") as f:
                    _serialize_schema(self.schema, f)
                    f.write(to_unsigned_int(0))
            return self
        first_row = data[0]
        self.schema = [(key, ColumnType.of(value)) for key, value in first_row.items()]
        columns_data = tuple([row[col_name] for row in data] for (col_name, _) in self.schema)
        return self._write_data_with_known_schema(columns_data, self.schema)

    def _write_data_with_known_schema(self, columns_data: Columns, schema: Schema) -> Self:
        assert schema
        self._block_starts = None
        with self.file.open(mode="wb") as f:
            _serialize_schema(schema, f)
            block_starts = []
            for data_block in _generate_data_blocks_for_columns(schema, columns_data):
                block_starts.append(f.tell())
                f.write(data_block)
            for block_start in block_starts:
                f.write(to_unsigned_int(block_start, LONG_BYTE_COUNT))
            f.write(to_unsigned_int(len(block_starts)))
        return self

    def append_data(self, data: Columns) -> Self:
        self._block_starts = None
        if not self.file.exists() or len(self.block_starts) == 0:
            return self.write_data(data)
        block_starts = self.block_starts
        schema = self.file_schema
        assert self.schema == self.file_schema, (self.file, self.schema, self.file_schema)
        with self.file.open(mode="rb+") as f:
            last_block = self.read_block_data_columns_by_id(len(self.block_starts) - 1, f)
            if len(last_block[0]) < ROWS_PER_BLOCK:
                data = merge_data_blocks(last_block, data)
                f.seek(block_starts.pop(), os.SEEK_SET)
            else:
                # seek to right after last block
                f.seek(-LONG_BYTE_COUNT * (len(block_starts) + 1), os.SEEK_END)
            for data_block in _generate_data_blocks_for_columns(schema, data):
                block_starts.append(f.tell())
                f.write(data_block)
            for block_size in block_starts:
                f.write(to_unsigned_int(block_size, LONG_BYTE_COUNT))
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

    def read_data_rows(self) -> Iterable[Row]:
        for block in self.read_blocks_sequentially():
            yield from block

    def read_block_data(self, block_id: int) -> list[tuple[Any, ...]]:
        block_data_columns = self.read_block_data_columns_by_id(block_id)
        return list(zip(*block_data_columns, strict=True))

    def read_block_data_columns_by_id(self, block_id: int, f: BufferedRandom | None = None) -> Columns:
        schema = self.file_schema
        block_start = self.block_starts[block_id]
        if f is not None:
            f.seek(block_start)
            return _deserialize_block(f, schema)
        with self.file.open("rb") as file:
            file.seek(block_start)
            return _deserialize_block(file, schema)

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

    def merge_files(self, files: list[Path]) -> Self:
        self.schema = BlockFile(files[0]).file_schema
        assert all(BlockFile(f).file_schema == self.schema for f in files)
        for file in files:
            block_file = BlockFile(file)
            for i in range(len(block_file.block_starts)):
                block_data = block_file.read_block_data_columns_by_id(i)
                self.append_data(block_data)
        return self

    def rows(self) -> int:
        total_rows = 0
        with self.file.open("rb") as f:
            for block_start in self.block_starts:
                f.seek(block_start)
                total_rows += read_unsigned_int(f)
        return total_rows
