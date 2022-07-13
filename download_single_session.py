import json
from micron.services import ConsumerService
from pathlib import Path

dataset = "covid19_aha"
table = "hospital_beds"
service = ConsumerService()
session = service.get_session(dataset, table)
fname, schema = service.download(f"{dataset}/{table}", session.streams[0].name)

with Path(f"{dataset}/{table}/schema.json").open("w") as f:
    f.write(json.dumps(schema))
