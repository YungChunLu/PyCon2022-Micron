import json
import concurrent.futures
from pathlib import Path
from micron.services import ConsumerService, PubSubService
from google.cloud.pubsub_v1.subscriber.message import Message

service = PubSubService()


def callback(message: Message):
    payload = json.loads(message.data.decode("utf-8"))
    dataset, table = payload["dataset"], payload["table"]
    max_stream_count = 2
    service = ConsumerService()
    session = service.get_session(dataset, table, max_stream_count=max_stream_count)
    fnames = []
    schema = None

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_stream_count
    ) as executor:
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

    ack_future = message.ack_with_response()
    ack_future.result(timeout=3.0)
    print(f"Succeed to ack the message. {message.message_id}")


service.start(callback)
