from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

OutputFileTuple = tuple[str, str, int]
JobResultTuple = tuple[str, str, list[OutputFileTuple]]


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


@dataclass
class Job:
    id: str = ""

    def __post_init__(self) -> None:
        self.id = str(uuid4())


@dataclass(kw_only=True)
class ScanJob(Job):
    file_path: Path
    block_id: int


@dataclass(kw_only=True)
class LoadShuffleFilesJob(Job):
    shuffle_files: list[OutputFile]


@dataclass(kw_only=True)
class JoinJob(Job):
    left_shuffle_files: list[OutputFile]
    right_shuffle_files: list[OutputFile]


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
