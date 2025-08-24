from .io import BlockFile
from .constants import Row
from typing import Any, Callable, TypeVar, Iterator
from pathlib import Path
from heapq import heappop, heappush

SORT_BLOCK_SIZE = 10 * 1024**2  # 10 MB

T = TypeVar("T")


def external_sort(
    file_to_sort: Path,
    key_col: Callable[[Row], Any],
    output_file: Path,
    temporary_file: Path,
):
    merge_file = BlockFile(temporary_file, SORT_BLOCK_SIZE)
    for block in BlockFile(file_to_sort, SORT_BLOCK_SIZE).read_blocks_sequentially():
        block.sort(key=key_col)
        merge_file.append_rows(block)

    iterators = [
        iter(merge_file.create_block_reader(block_start, row_buffer_size=1000))
        for block_start in merge_file.block_starts
    ]

    output_block_file = BlockFile(output_file, SORT_BLOCK_SIZE)

    def write_merge_result(rows: list[Row]):
        output_block_file.append_rows(rows)

    kway_merge(
        iterators, key_col, write_merge_result, action_trigger_size=SORT_BLOCK_SIZE
    )


def kway_merge(
    iterators: list[Iterator[T]],
    key: Callable[[T], Any],
    action: Callable[[list[T]], Any],
    action_trigger_size: int,
) -> None:
    counter = 0
    # initialize heap
    heap: list[tuple[Any, int, T, Iterator[T]]] = []
    for iter in iterators:
        try:
            curr_el = next(iter)
            heappush(heap, (key(curr_el), counter, curr_el, iter))
            counter += 1
        except StopIteration:
            continue
    # merge
    curr_result: list[T] = []
    while heap:
        _, _, curr_el, iter = heappop(heap)
        curr_result.append(curr_el)
        if len(curr_result) >= action_trigger_size:
            action(curr_result)
            curr_result = []
        try:
            next_el = next(iter)
            heappush(heap, (key(next_el), counter, next_el, iter))
            counter += 1
        except StopIteration:
            continue
    if curr_result:
        action(curr_result)


def external_merge_join(files_to_sort, key_col: Callable[[Row], Any]):
    pass
