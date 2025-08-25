from typing import Any
from pathlib import Path
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


USE_WORKERS = False
SHUFFLE_FOLDER = Path("shuffle/")
BLOCK_SIZE = 10 * 1024 * 1024  # 10 MB
Row = dict[str, Any]
Schema = list[tuple[str, ColumnType]]
