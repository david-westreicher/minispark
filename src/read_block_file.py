from itertools import islice
from pathlib import Path

from tabulate import tabulate

from mini_spark.io import BlockFile

if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(
        description="Read a block file and print its contents.",
    )
    arg_parser.add_argument(
        "file_path",
        type=Path,
        help="Path to the block file to read.",
    )
    args = arg_parser.parse_args()

    block_file = BlockFile(args.file_path)
    print(f"Rows: {block_file.rows()}")  # noqa: T201
    print(block_file.block_starts)  # noqa: T201
    rows = list(islice(block_file.read_data_rows(), 100))
    print(tabulate(rows, tablefmt="rounded_outline", headers="keys"))  # noqa: T201
