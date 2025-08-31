from __future__ import annotations

import atexit
import functools
import pickle
import time
from collections.abc import Callable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, TypeVar

from perfetto.protos.perfetto.trace.perfetto_trace_pb2 import TrackEvent
from perfetto.trace_builder.proto_builder import TraceProtoBuilder

from .constants import GLOBAL_TEMP_FOLDER, Columns, Row, Schema

if TYPE_CHECKING:
    from collections.abc import Iterable

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")
TRUSTED_PACKET_SEQUENCE_ID = 0
NESTED_SLICE_TRACK_UUID = 12
MAIN_SYSTEM_TRACK_UUID = 123123


def create_temp_file() -> Path:
    with NamedTemporaryFile(dir=GLOBAL_TEMP_FOLDER, delete=True) as f:
        tmp_file_name = f.name
    return Path(tmp_file_name)


def nice_schema(schema: Schema | None) -> str:
    if schema is None:
        return "None"
    return "[" + ", ".join(f"{name}:{type_.name}" for name, type_ in schema) + "]"


def convert_rows_to_columns(rows: Iterable[Row], schema: Schema) -> Columns:
    return tuple([row[col] for row in rows] for col, _ in schema)


class Tracer:
    def __init__(self) -> None:
        self.trace_proto = TraceProtoBuilder()
        self.tracks: set[int] = set()
        self.define_custom_track(MAIN_SYSTEM_TRACK_UUID, "Main System")
        atexit.register(self.save, "current.pftrace")

    def unregister(self) -> None:
        atexit.unregister(self.save)

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

    def save(self, filename: str) -> None:
        with Path(filename).open("wb") as f:
            f.write(self.trace_proto.serialize())


class MyTracer:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def start(self, name: str) -> None:
        packet = {
            "timestamp": time.time_ns(),
            "track_event.type": TrackEvent.TYPE_SLICE_BEGIN,
            "track_event.name": name,
        }
        self.events.append(packet)

    def end(self) -> None:
        packet = {
            "timestamp": time.time_ns(),
            "track_event.type": TrackEvent.TYPE_SLICE_END,
        }
        self.events.append(packet)

    def save(self, filename: str) -> None:
        if self.events:
            with Path(filename).open("wb") as f:
                pickle.dump(self.events, f)


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


def collect_and_trace_worker_traces(worker_count: int) -> None:
    for worker_id in range(worker_count):
        worker_trace_file = Path(f"worker-{worker_id}.pftrace")
        try:
            with worker_trace_file.open("rb") as f:
                TRACER.define_custom_track(
                    worker_id + 1,
                    name=f"Worker {worker_id}",
                    parent_track_uuid=MAIN_SYSTEM_TRACK_UUID,
                )
                events = pickle.load(f)  # TODO(david): don't use pickle  # noqa: S301
                for event in events:
                    packet = TRACER.trace_proto.add_packet()
                    packet.timestamp = event["timestamp"]
                    packet.track_event.type = event["track_event.type"]
                    packet.track_event.track_uuid = worker_id + 1
                    if "track_event.name" in event:
                        packet.track_event.name = event["track_event.name"]
                    packet.trusted_packet_sequence_id = TRUSTED_PACKET_SEQUENCE_ID
            worker_trace_file.unlink()
        except FileNotFoundError:
            pass


TRACER = Tracer()


@trace("convert_col_to_rows")
def convert_columns_to_rows(cols: Columns, schema: Schema) -> Iterable[Row]:
    for row in zip(*cols, strict=True):
        yield {name: val for val, (name, _) in zip(row, schema, strict=True)}


def chunk_list[T](lst: list[T], n_chunks: int) -> list[list[T]]:
    if not lst:
        return []
    n = len(lst) // n_chunks
    return [lst[i : i + n] for i in range(0, len(lst), n)]
