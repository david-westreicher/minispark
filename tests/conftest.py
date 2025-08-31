from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock

import pytest

from mini_spark import utils


@pytest.fixture(autouse=True)
def setup_function(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    shuffle_folder = tmp_path / "shuffle"
    shuffle_folder.mkdir()
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr("mini_spark.constants.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.tasks.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.utils.GLOBAL_TEMP_FOLDER", tmp_path / "tmp")
    monkeypatch.setattr("mini_spark.execution.GLOBAL_TEMP_FOLDER", tmp_path / "tmp")


@pytest.fixture(autouse=True)
def patch_tracer(monkeypatch: pytest.MonkeyPatch):
    mock_tracer = MagicMock()
    utils.TRACER.unregister()
    monkeypatch.setattr("mini_spark.utils.TRACER", mock_tracer)
    monkeypatch.setattr("mini_spark.execution.TRACER", mock_tracer)
    return mock_tracer


@pytest.fixture
def temporary_file() -> Iterable[Path]:
    with NamedTemporaryFile(delete=True) as tmp:
        yield Path(tmp.name)
