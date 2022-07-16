import google.auth
import os
from typing import Callable, Any
from google.cloud.pubsub import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl, message


class PubSubService:
    def __init__(self) -> None:
        self.subscriber = SubscriberClient()
        project_id = os.environ.get("PUBSUB_PROJECT_ID")
        if project_id is None:
            _, self.project_id = google.auth.default()
        else:
            self.project_id = project_id
        self.subscription = f"projects/{self.project_id}/subscriptions/{os.environ.get('SUBSCRIPTION_ID', 'pycon-consumer')}"
        self.max_messages = 1  # the maximun number of messages per pull
        self.max_lease_duration = 3600  # the timeout of a message lease
        self.min_duration_per_lease_extension = (
            600  # the min extension of a message lease in secods per extend
        )

    def start(self, callback: Callable[["message.Message"], Any]) -> None:
        """Streaming pull messages for the subscription."""
        streaming_pull_future = self.subscriber.subscribe(
            subscription=self.subscription,
            callback=callback,
            flow_control=FlowControl(
                max_messages=self.max_messages,
                max_lease_duration=self.max_lease_duration,
                min_duration_per_lease_extension=self.min_duration_per_lease_extension,
            ),
        )
        with self.subscriber:
            try:
                streaming_pull_future.result()
            except Exception:
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.
