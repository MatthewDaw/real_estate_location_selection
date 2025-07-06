from google.cloud import pubsub_v1
import json
import time
from typing import Optional, Dict, Any


class TopicManager:
    """Synchronous Pub/Sub topic management with 30-minute ack deadline"""

    def __init__(self, topic_name:str, project_id: str, subscription_name: str, dead_letter_topic: str = None):
        self.topic_name = topic_name
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.dead_letter_topic = dead_letter_topic

        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
        if dead_letter_topic:
            self.dead_letter_topic_path = self.publisher.topic_path(project_id, dead_letter_topic)

    def pull_message(self):
        """
        Generator that yields message data and automatically handles cleanup.
        On successful completion: deletes the message
        On exception: sends to dead letter queue (if configured)
        """
        pull_request = {
            "subscription": self.subscription_path,
            "max_messages": 1
        }

        response = self.subscriber.pull(request=pull_request)

        if not response.received_messages:
            return

        received_message = response.received_messages[0]
        message = received_message.message

        try:
            # Try to parse as JSON, fall back to string
            data = json.loads(message.data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = message.data.decode('utf-8', errors='ignore')

        message_data = {
            'data': data,
            'attributes': dict(message.attributes),
            'message_id': message.message_id,
            'publish_time': message.publish_time,
            'ack_id': received_message.ack_id
        }

        try:
            # Yield the message for processing
            yield message_data
            # If we get here, processing was successful - delete the message
            self.delete_message(message_data['ack_id'])

        except Exception as e:
            print(f"Error processing message {message_data['message_id']}: {e}")
            # Send to DLQ if configured, otherwise just delete
            if self.dead_letter_topic:
                self.delete_and_send_to_dlq(message_data)
            else:
                self.delete_message(message_data['ack_id'])
            raise  # Re-raise the exception

    def delete_message(self, ack_id: str) -> bool:
        """
        Delete (acknowledge) a message from the subscription.
        Returns True if successful.
        """
        try:
            self.subscriber.acknowledge(
                subscription=self.subscription_path,
                ack_ids=[ack_id]
            )
            return True
        except Exception as e:
            print(f"Error deleting message: {e}")
            return False

    def delete_and_send_to_dlq(self, message_data: Dict[str, Any]) -> bool:
        """
        Delete a message and send it to the dead letter queue.
        Returns True if both operations succeed.
        """
        if not self.dead_letter_topic:
            raise ValueError("Dead letter topic not configured")

        try:
            # Send to dead letter queue first
            dlq_data = {
                'original_data': message_data['data'],
                'original_attributes': message_data['attributes'],
                'original_message_id': message_data['message_id'],
                'original_publish_time': str(message_data['publish_time']),
                'reason': 'moved_to_dlq'
            }

            # Publish to dead letter queue
            future = self.publisher.publish(
                self.dead_letter_topic_path,
                json.dumps(dlq_data).encode('utf-8')
            )
            future.result()  # Wait for publish to complete

            # Delete original message
            success = self.delete_message(message_data['ack_id'])

            if success:
                print(f"Message {message_data['message_id']} moved to dead letter queue")

            return success

        except Exception as e:
            print(f"Error moving message to DLQ: {e}")
            return False

    def empty_topic(self) -> int:
        """
        Empties all messages from the current subscription (`self.subscription_path`)
        by pulling and acknowledging them in batches.

        Returns:
            int: Number of messages deleted.
        """
        total = 0
        while True:
            response = self.subscriber.pull(
                request={
                    "subscription": self.subscription_path,
                    "max_messages": 100
                }
            )

            if not response.received_messages:
                break

            ack_ids = [msg.ack_id for msg in response.received_messages]
            self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=ack_ids)

            total += len(ack_ids)
            print(f"Deleted {len(ack_ids)} messages (total: {total})")

        print(f"✅ Emptied subscription '{self.subscription_name}': {total} messages deleted")
        return total
