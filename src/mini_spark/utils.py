from __future__ import annotations

import csv
import functools
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path
from queue import Empty
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, TypeVar

from perfetto.protos.perfetto.trace.perfetto_trace_pb2 import TrackEvent
from perfetto.trace_builder.proto_builder import TracePacket, TraceProtoBuilder

from .constants import GLOBAL_TEMP_FOLDER, ROWS_PER_BLOCK, Columns, ColumnType, Row, Schema
from .io import BlockFile

if TYPE_CHECKING:
    from collections.abc import Iterable

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")
TRUSTED_PACKET_SEQUENCE_ID = 0
MAIN_SYSTEM_TRACK_UUID = 123123


def create_temp_file() -> Path:
    with NamedTemporaryFile(dir=GLOBAL_TEMP_FOLDER, delete=True) as f:
        tmp_file_name = f.name
    return Path(tmp_file_name)


def nice_schema(schema: Schema | None) -> str:
    if schema is None:
        return "None"
    return "[" + ", ".join(f"{name}:{type_.name}" for name, type_ in schema) + "]"


def convert_columns_to_rows(rows: Columns, schema: Schema) -> Iterable[Row]:
    for i in range(len(rows[0])):
        yield {schema[j][0]: rows[j][i] for j in range(len(schema))}


@dataclass
class TraceEvent:
    name: str
    timestamp: int
    is_start: bool

    def set_packet(self, packet: TracePacket, track_uuid: int = -1) -> None:
        packet.timestamp = self.timestamp
        packet.track_event.type = TrackEvent.TYPE_SLICE_BEGIN if self.is_start else TrackEvent.TYPE_SLICE_END
        packet.track_event.track_uuid = MAIN_SYSTEM_TRACK_UUID if track_uuid == -1 else track_uuid
        packet.trusted_packet_sequence_id = TRUSTED_PACKET_SEQUENCE_ID
        if self.is_start:
            packet.track_event.name = self.name


@dataclass
class TraceFile:
    timestamp: int
    trace_file: Path
    worker_id: str
    parent_uuid_track: int

    def parse(self) -> Iterable[TraceEvent]:
        try:
            with self.trace_file.open("rb") as f:
                while True:
                    is_start = int(f.read(1)[0])
                    timestamp = int.from_bytes(f.read(8), byteorder="little", signed=False)
                    name_len = int(f.read(1)[0])
                    name = f.read(name_len).decode("utf-8")
                    yield TraceEvent(name, timestamp + self.timestamp, is_start == 0)
        except (EOFError, IndexError):
            pass


TRACE_FILES: Queue[TraceFile] = Queue()


class Tracer:
    def __init__(self) -> None:
        self.trace_proto = TraceProtoBuilder()
        self.tracks: set[int] = set()
        self.define_custom_track(MAIN_SYSTEM_TRACK_UUID, "Main System")

    def new_track(self, name: str, parent_track_uuid: int = MAIN_SYSTEM_TRACK_UUID) -> int:
        track_uuid = hash(name) % 100000 + 1000
        self.define_custom_track(track_uuid, name, parent_track_uuid)
        return track_uuid

    def define_custom_track(self, track_uuid: int, name: str, parent_track_uuid: int | None = None) -> None:
        if track_uuid in self.tracks:
            return
        packet = self.trace_proto.add_packet()
        desc = packet.track_descriptor
        desc.uuid = track_uuid
        desc.name = name
        if parent_track_uuid:
            desc.parent_uuid = parent_track_uuid
        self.tracks.add(track_uuid)

    def start(self, name: str, track_uuid: int = -1) -> None:
        packet = self.trace_proto.add_packet()
        packet.timestamp = time.time_ns()
        packet.track_event.type = TrackEvent.TYPE_SLICE_BEGIN
        packet.track_event.track_uuid = MAIN_SYSTEM_TRACK_UUID if track_uuid == -1 else track_uuid
        packet.track_event.name = name
        packet.trusted_packet_sequence_id = TRUSTED_PACKET_SEQUENCE_ID

    def end(self, track_uuid: int = -1) -> None:
        packet = self.trace_proto.add_packet()
        packet.timestamp = time.time_ns()
        packet.track_event.type = TrackEvent.TYPE_SLICE_END
        packet.track_event.track_uuid = MAIN_SYSTEM_TRACK_UUID if track_uuid == -1 else track_uuid
        packet.trusted_packet_sequence_id = TRUSTED_PACKET_SEQUENCE_ID

    def add_trace_file(self, trace_file: Path, worker_id: str, parent_track_uuid: int) -> None:
        TRACE_FILES.put(TraceFile(time.time_ns(), trace_file, worker_id, parent_track_uuid))

    def save(self, filename: str) -> None:
        try:
            while trace_file := TRACE_FILES.get(timeout=0.1):
                track_uuid = self.new_track(f"Worker {trace_file.worker_id}", trace_file.parent_uuid_track)
                for event in trace_file.parse():
                    event.set_packet(self.trace_proto.add_packet(), track_uuid)
        except Empty:
            pass

        with Path(filename).open("wb") as f:
            f.write(self.trace_proto.serialize())


def trace(block_name: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def] # noqa: ANN002, ANN003, ANN202
            TRACER.start(block_name)
            try:
                result = func(*args, **kwargs)
            finally:
                TRACER.end()
            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def trace_yield(block_name: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def] # noqa: ANN002, ANN003, ANN202
            TRACER.start(block_name)
            try:
                yield from func(*args, **kwargs)
            finally:
                TRACER.end()

        return wrapper  # type: ignore[return-value]

    return decorator


TRACER = Tracer()


def chunk_list[T](lst: list[T], n_chunks: int) -> list[list[T]]:
    if not lst:
        return []
    n = len(lst) // n_chunks
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def convert_csv_to_block_file(
    csv_file: Path,
    block_file: Path,
    schema: Schema,
    batch_size: int = ROWS_PER_BLOCK,
) -> None:
    if block_file.exists():
        raise FileExistsError(f"File {block_file} already exists")
    with csv_file.open() as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        while True:
            rows = []
            for row_count, row in enumerate(reader):
                out_row = []
                for el, (_, col_type) in zip(row, schema, strict=True):
                    out = datetime.fromisoformat(el) if col_type == ColumnType.TIMESTAMP else col_type.type(el)
                    out_row.append(out)
                rows.append(tuple(out_row))
                if row_count >= batch_size:
                    break
            else:
                BlockFile(block_file, schema).append_tuples(rows)
                break
            BlockFile(block_file, schema).append_tuples(rows)
