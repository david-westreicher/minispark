from typing import Any
from pathlib import Path

USE_WORKERS = False
SHUFFLE_FOLDER = Path("shuffle/")
BLOCK_SIZE = 10 * 1024 * 1024  # 10 MB
Row = dict[str, Any]
