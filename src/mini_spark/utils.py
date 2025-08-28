import atexit
import functools
import os
import pickle
import time
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile

from perfetto.protos.perfetto.trace.perfetto_trace_pb2 import TrackEvent
from perfetto.trace_builder.proto_builder import TraceProtoBuilder

from .constants import GLOBAL_TEMP_FOLDER, Columns, Row, Schema

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
    def __init__(self):
        self.trace_proto = TraceProtoBuilder()
        self.tracks = set()
        self.define_custom_track(MAIN_SYSTEM_TRACK_UUID, "Main System")
        atexit.register(self.save, "current.pftrace")

    def unregister(self):
        atexit.unregister(self.save)

    def define_custom_track(self, track_uuid, name, parent_track_uuid=None):
        if track_uuid in self.tracks:
            return
        print("define track", track_uuid, name)
        packet = self.trace_proto.add_packet()
        desc = packet.track_descriptor
        desc.uuid = track_uuid
        desc.name = name
        if parent_track_uuid:
            desc.parent_uuid = parent_track_uuid
        self.tracks.add(track_uuid)

    def start(self, name: str, track_uuid: int = -1):
        packet = self.trace_proto.add_packet()
        packet.timestamp = time.time_ns()
        packet.track_event.type = TrackEvent.TYPE_SLICE_BEGIN
        packet.track_event.track_uuid = (
            MAIN_SYSTEM_TRACK_UUID if track_uuid == -1 else track_uuid
        )
        packet.track_event.name = name
        packet.trusted_packet_sequence_id = TRUSTED_PACKET_SEQUENCE_ID

    def end(self, track_uuid: int = -1):
        packet = self.trace_proto.add_packet()
        packet.timestamp = time.time_ns()
        packet.track_event.type = TrackEvent.TYPE_SLICE_END
        packet.track_event.track_uuid = (
            MAIN_SYSTEM_TRACK_UUID if track_uuid == -1 else track_uuid
        )
        packet.trusted_packet_sequence_id = TRUSTED_PACKET_SEQUENCE_ID

    def save(self, filename: str):
        print("save")
        with open(filename, "wb") as f:
            f.write(self.trace_proto.serialize())


class MyTracer:
    def __init__(self):
        self.events = []

    def start(self, name: str):
        packet = {
            "timestamp": time.time_ns(),
            "track_event.type": TrackEvent.TYPE_SLICE_BEGIN,
            "track_event.name": name,
        }
        self.events.append(packet)

    def end(self):
        packet = {
            "timestamp": time.time_ns(),
            "track_event.type": TrackEvent.TYPE_SLICE_END,
        }
        self.events.append(packet)

    def save(self, filename: str):
        if self.events:
            with open(filename, "wb") as f:
                pickle.dump(self.events, f)
        else:
            print("No events to save", filename)


def trace(block_name: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            TRACER.start(block_name)
            try:
                result = func(*args, **kwargs)
            finally:
                TRACER.end()
            return result

        return wrapper

    return decorator


def collect_and_trace_worker_traces(worker_count: int):
    print("collect trace files")
    for worker_id in range(worker_count):
        worker_trace_file = f"worker-{worker_id}.pftrace"
        try:
            with open(worker_trace_file, "rb") as f:
                print(worker_trace_file)
                TRACER.define_custom_track(
                    worker_id + 1,
                    name=f"Worker {worker_id}",
                    parent_track_uuid=MAIN_SYSTEM_TRACK_UUID,
                )
                events = pickle.load(f)
                for event in events:
                    packet = TRACER.trace_proto.add_packet()
                    packet.timestamp = event["timestamp"]
                    packet.track_event.type = event["track_event.type"]
                    packet.track_event.track_uuid = worker_id + 1
                    if "track_event.name" in event:
                        packet.track_event.name = event["track_event.name"]
                    packet.trusted_packet_sequence_id = TRUSTED_PACKET_SEQUENCE_ID
            os.unlink(worker_trace_file)
        except FileNotFoundError:
            print(f"Worker trace file {worker_trace_file} not found.")


TRACER = Tracer()


@trace("convert_col_to_rows")
def convert_columns_to_rows(cols: Columns, schema: Schema) -> Iterable[Row]:
    for row in zip(*cols):
        yield {name: val for val, (name, _) in zip(row, schema)}
