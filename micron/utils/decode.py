import argparse
import json
from fastavro import schemaless_reader, parse_schema
from fastavro.types import Schema
from pathlib import Path
from typing import IO


def records(buf: IO, schema: Schema):
    while True:
        try:
            yield schemaless_reader(buf, schema)
        except StopIteration:
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A util to decode an avro file.")

    parser.add_argument("--file", required=True, help="the file path")
    parser.add_argument("--rows", default=3, help="the number of peeked rows")
    args = parser.parse_args()

    avro_fpath = Path(args.file)
    if not avro_fpath.is_file():
        raise FileNotFoundError()

    schema_fpath = avro_fpath.parent / "schema.json"
    with schema_fpath.open("r") as f:
        schema: Schema = json.loads(f.read())

    with avro_fpath.open("rb") as fo:
        count = 0
        for record in records(fo, parse_schema(schema)):
            if count > args.rows:
                break
            print(record)
            count += 1
