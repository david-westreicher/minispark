from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PosixPath  # noqa: F401 PosixPath is used in eval
from typing import cast
from uuid import uuid4

OutputFileTuple = tuple[str, str, int]
JobResultTuple = tuple[str, str, list[OutputFileTuple]]


@dataclass
class Job:
    id: str = ""

    def __post_init__(self) -> None:
        self.id = str(uuid4())

    def cmd_args(self) -> list[str]:
        raise NotImplementedError


@dataclass
class JobResult:
    job_id: str
    executor_id: str
    output_files: list[OutputFile]

    @staticmethod
    def from_tuple(t: JobResultTuple) -> JobResult:
        return JobResult(t[0], t[1], [OutputFile.from_tuple(f) for f in t[2]])

    def to_tuple(self) -> JobResultTuple:
        return (self.job_id, self.executor_id, [f.to_tuple() for f in self.output_files])


@dataclass(frozen=True)
class OutputFile:
    executor_id: str
    file_path: Path
    partition: int = 0

    @staticmethod
    def from_tuple(t: OutputFileTuple) -> OutputFile:
        return OutputFile(t[0], Path(t[1]), t[2])

    def to_tuple(self) -> OutputFileTuple:
        return (self.executor_id, str(self.file_path), self.partition)


@dataclass(kw_only=True)
class ScanJob(Job):
    file_path: Path
    block_id: int

    def cmd_args(self) -> list[str]:
        return ["0", str(self.file_path.absolute()), str(self.block_id)]


@dataclass(kw_only=True)
class LoadShuffleFilesJob(Job):
    shuffle_files: list[OutputFile]

    def cmd_args(self) -> list[str]:
        shuffle_files = set(self.shuffle_files)
        return ["1", str(len(shuffle_files))] + [str(f.file_path.absolute()) for f in shuffle_files]


@dataclass(kw_only=True)
class JoinJob(Job):
    left_shuffle_files: list[OutputFile]
    right_shuffle_files: list[OutputFile]


@dataclass
class RemoteJob:
    original_job: Job
    stage_id: str
    output_file: Path = Path()
    executor_binary: Path = Path()

    def serialize(self) -> str:
        return repr(self)

    @staticmethod
    def deserialize(t: str) -> RemoteJob:
        return cast("RemoteJob", eval(t))  # noqa: S307
