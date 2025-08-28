import pytest
from pathlib import Path
from typing import Iterable
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock
from mini_spark import utils


@pytest.fixture(scope="function", autouse=True)
def setup_function(tmp_path: Path, monkeypatch):
    shuffle_folder = tmp_path / "shuffle"
    shuffle_folder.mkdir()
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr("mini_spark.constants.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.tasks.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.utils.GLOBAL_TEMP_FOLDER", tmp_path / "tmp")


@pytest.fixture(autouse=True)
def patch_tracer(monkeypatch):
    mock_tracer = MagicMock()
    utils.TRACER.unregister()
    monkeypatch.setattr("mini_spark.utils.TRACER", mock_tracer)
    monkeypatch.setattr("mini_spark.query.TRACER", mock_tracer)
    return mock_tracer


@pytest.fixture
def temporary_file() -> Iterable[Path]:
    with NamedTemporaryFile(delete=True) as tmp:
        yield Path(tmp.name)
