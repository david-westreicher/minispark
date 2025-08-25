from pathlib import Path
from tempfile import NamedTemporaryFile
from .constants import Schema

GLOBAL_TEMP_FOLDER = Path("tmp/")


def create_temp_file() -> Path:
    with NamedTemporaryFile(dir=GLOBAL_TEMP_FOLDER, delete=True) as f:
        tmp_file_name = f.name
    return Path(tmp_file_name)


def nice_schema(schema: Schema | None) -> str:
    if schema is None:
        return "None"
    return "[" + ", ".join(f"{name}:{type_.name}" for name, type_ in schema) + "]"
