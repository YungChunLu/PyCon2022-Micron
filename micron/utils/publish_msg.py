import json
import os
import argparse
from google.cloud.pubsub import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A util to publish message.")

    parser.add_argument("--dataset", required=True, help="the source dataset name")
    parser.add_argument("--table", required=True, help="the source table name")
    args = parser.parse_args()

    topic_id = os.getenv("TOPIC_ID", "pycon")
    project_id = os.getenv("PUBSUB_PROJECT_ID", "local-dev")
    client = PublisherClient()
    topic = client.topic_path(project=project_id, topic=topic_id)

    payload = {"dataset": args.dataset, "table": args.table}
    msg = json.dumps(payload).encode("utf-8")
    future: Future = client.publish(topic=topic, data=msg)
    future.result(timeout=3.0)
