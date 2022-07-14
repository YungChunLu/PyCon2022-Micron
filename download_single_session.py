import json
from micron.services import ConsumerService
from pathlib import Path

dataset = "crypto_bitcoin"
table = "blocks"
service = ConsumerService()
session = service.get_session(dataset, table)
fname, schema = service.download(f"{dataset}/{table}", session.streams[0].name)  # type: ignore

with Path(f"{dataset}/{table}/schema.json").open("w") as f:
    f.write(json.dumps(schema))

print(f"File name: {fname}")
