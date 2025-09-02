from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4


@dataclass(frozen=True)
class OutputFile:
    executor_id: str
    file_path: Path
    partition: int


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
