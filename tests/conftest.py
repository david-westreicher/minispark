import pytest
from pathlib import Path
from mini_spark import query
from typing import Iterable
from tempfile import NamedTemporaryFile


@pytest.fixture(scope="session", autouse=True)
def setup_global():
    query.USE_WORKERS = False


@pytest.fixture(scope="function", autouse=True)
def setup_function(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("mini_spark.constants.USE_WORKERS", False)
    shuffle_folder = tmp_path / "shuffle"
    shuffle_folder.mkdir()
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr("mini_spark.constants.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.tasks.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.utils.GLOBAL_TEMP_FOLDER", tmp_path / "tmp")


@pytest.fixture
def temporary_file() -> Iterable[Path]:
    with NamedTemporaryFile(delete=True) as tmp:
        yield Path(tmp.name)
