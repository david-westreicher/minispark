from __future__ import annotations

from enum import Enum
from pathlib import Path

ROWS_PER_BLOCK = 2 * 1024 * 1024
SHUFFLE_PARTITIONS = 10
WORKER_POOL_PROCESSES = 4
GLOBAL_TEMP_FOLDER = Path("tmp/")
SHUFFLE_FOLDER = Path("shuffle/")
DEBUG_EXECUTION = False

MAX_INT = 2**31 - 1
MIN_INT = -(2**31)


class ColumnType(Enum):
    INTEGER = (0, int)
    STRING = (1, str)
    FLOAT = (2, float)
    UNKNOWN = (255, type(None))

    def __init__(self, value: int, py_type: type) -> None:
        self.ordinal = value
        self.type = py_type

    @staticmethod
    def from_ordinal(ordinal: int) -> ColumnType:
        for col_type in ColumnType:
            if col_type.ordinal == ordinal:
                return col_type
        raise NotImplementedError(ordinal)

    @staticmethod
    def of(value: ColumnTypePython) -> ColumnType:
        if type(value) is int:
            return ColumnType.INTEGER
        if type(value) is str:
            return ColumnType.STRING
        if type(value) is float:
            return ColumnType.FLOAT
        return ColumnType.UNKNOWN

    @property
    def native_zig_type(self) -> str:
        if self == ColumnType.INTEGER:
            return "i32"
        if self == ColumnType.STRING:
            return "[]const u8"
        if self == ColumnType.FLOAT:
            return "f32"
        raise NotImplementedError(self)

    @property
    def zig_type(self) -> str:
        if self == ColumnType.INTEGER:
            return "I32"
        if self == ColumnType.STRING:
            return "Str"
        if self == ColumnType.FLOAT:
            return "F32"
        raise NotImplementedError(self)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.__str__()


ColumnTypePython = int | float | str
Row = dict[str, ColumnTypePython]
Columns = tuple[list[ColumnTypePython], ...]
Schema = list[tuple[str, ColumnType]]
