from pathlib import Path

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

    BlockFile(Path("some_test.bin")).write_rows(
        [
            {"delta": -1, "msg": "hello"},
            {"delta": 2, "msg": "zig"},
            {"delta": 3, "msg": "!"},
        ],
    )

    for row in BlockFile(args.file_path).read_data_rows():
        print(row)  # noqa: T201
