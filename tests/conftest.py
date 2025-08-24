import pytest
from pathlib import Path
from mini_spark import query
from typing import Iterable
from tempfile import NamedTemporaryFile


@pytest.fixture(scope="session", autouse=True)
def setup_global():
    query.USE_WORKERS = False


@pytest.fixture
def temporary_file() -> Iterable[Path]:
    with NamedTemporaryFile(delete=True) as tmp:
        yield Path(tmp.name)
