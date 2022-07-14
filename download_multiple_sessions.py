import json
import concurrent.futures
from micron.services import ConsumerService
from pathlib import Path

dataset = "crypto_bitcoin"
table = "blocks"
max_stream_count = 2
service = ConsumerService()
session = service.get_session(dataset, table, max_stream_count=max_stream_count)
fnames = []
schema = None

with concurrent.futures.ThreadPoolExecutor(max_workers=max_stream_count) as executor:
    futures = [
        executor.submit(service.download, f"{dataset}/{table}", stream.name)
        for stream in session.streams  # type: ignore
    ]

    for future in concurrent.futures.as_completed(futures):
        fname, schema = future.result()
        fnames.append(fname)

if schema is not None:
    with Path(f"{dataset}/{table}/schema.json").open("w") as f:
        f.write(json.dumps(schema))
