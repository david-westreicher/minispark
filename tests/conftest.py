import struct
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock

import pytest

from mini_spark.constants import Row


@pytest.fixture(autouse=True)
def setup_function(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    shuffle_folder = tmp_path / "shuffle"
    shuffle_folder.mkdir()
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr("mini_spark.constants.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.tasks.SHUFFLE_FOLDER", shuffle_folder)
    monkeypatch.setattr("mini_spark.utils.GLOBAL_TEMP_FOLDER", tmp_path / "tmp")
    monkeypatch.setattr("mini_spark.execution.WORKER_POOL_PROCESSES", 1)


@pytest.fixture(autouse=True)
def patch_tracer(monkeypatch: pytest.MonkeyPatch):
    mock_tracer = MagicMock()
    monkeypatch.setattr("mini_spark.utils.TRACER", mock_tracer)
    monkeypatch.setattr("mini_spark.execution.TRACER", mock_tracer)
    return mock_tracer


@pytest.fixture
def temporary_file() -> Iterable[Path]:
    with NamedTemporaryFile(delete=True) as tmp:
        yield Path(tmp.name)


def compare_float(left: float, right: float) -> float:
    def conver_to_float32(value: float) -> float:
        return float(struct.unpack("<f", struct.pack("<f", value))[0])

    return conver_to_float32(left) == conver_to_float32(right)


def assert_rows_equal(rows_0: list[Row], rows_1: list[Row]):
    # sort rows by all keys to ensure order doesn't matter
    rows_0 = sorted(rows_0, key=lambda r: tuple(r.values()))
    rows_1 = sorted(rows_1, key=lambda r: tuple(r.values()))
    for r0, r1 in zip(rows_0, rows_1, strict=True):
        assert r0.keys() == r1.keys(), f"Row keys mismatch: {r0.keys()} != {r1.keys()}"
        for key in r0:
            left, right = r0[key], r1[key]
            assert type(left) is type(right), f"Row value type mismatch for key '{key}': {type(left)} != {type(right)}"
            if type(left) is float and type(right) is float:
                assert compare_float(left, right), f"Row float value mismatch for key '{key}': {left} != {right}"
            else:
                assert left == right, f"Row value mismatch for key '{key}': {left} != {right}"
