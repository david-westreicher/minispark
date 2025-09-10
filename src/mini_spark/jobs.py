from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

from .io import encode_string, to_unsigned_int

if TYPE_CHECKING:
    from pathlib import Path

OutputFileTuple = tuple[str, str, int]
JobResultTuple = tuple[str, str, list[OutputFileTuple]]


@dataclass
class Job:
    id: str = ""

    def __post_init__(self) -> None:
        self.id = str(uuid4())

    def encode(self) -> bytes:
        raise NotImplementedError


@dataclass
class JobResult:
    job_id: str
    executor_id: str
    output_files: list[OutputFile]


@dataclass(frozen=True)
class OutputFile:
    file_path: Path
    partition: int = 0


@dataclass(kw_only=True)
class ScanJob(Job):
    file_path: Path
    block_id: int

    def encode(self) -> bytes:
        job_type = 0
        return bytes([job_type]) + encode_string(str(self.file_path.absolute())) + to_unsigned_int(self.block_id)


@dataclass(kw_only=True)
class LoadShuffleFilesJob(Job):
    shuffle_files: list[OutputFile]

    def encode(self) -> bytes:
        shuffle_files = set(self.shuffle_files)
        job_type = 1
        return (
            bytes([job_type])
            + to_unsigned_int(len(shuffle_files))
            + b"".join([encode_string(str(f.file_path.absolute())) for f in shuffle_files])
        )


@dataclass(kw_only=True)
class JoinJob(Job):
    left_shuffle_files: list[OutputFile]
    right_shuffle_files: list[OutputFile]
