from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable
from .constants import Schema, Row, Columns

GLOBAL_TEMP_FOLDER = Path("tmp/")


def create_temp_file() -> Path:
    with NamedTemporaryFile(dir=GLOBAL_TEMP_FOLDER, delete=True) as f:
        tmp_file_name = f.name
    return Path(tmp_file_name)


def nice_schema(schema: Schema | None) -> str:
    if schema is None:
        return "None"
    return "[" + ", ".join(f"{name}:{type_.name}" for name, type_ in schema) + "]"


def convert_columns_to_rows(cols: Columns, schema: Schema) -> Iterable[Row]:
    for row in zip(*cols):
        yield {name: val for val, (name, _) in zip(row, schema)}


def convert_rows_to_columns(rows: Iterable[Row], schema: Schema) -> Columns:
    return tuple([row[col] for row in rows] for col, _ in schema)
