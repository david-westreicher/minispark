from pathlib import Path
from tempfile import NamedTemporaryFile

GLOBAL_TEMP_FOLDER = Path("tmp/")


def create_temp_file() -> Path:
    with NamedTemporaryFile(dir=GLOBAL_TEMP_FOLDER, delete=True) as f:
        tmp_file_name = f.name
    return Path(tmp_file_name)
