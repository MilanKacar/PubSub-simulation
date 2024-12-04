from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import threading
import json
import os

class KafkaPubSubManager:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except NoBrokersAvailable as e:
            print(f"Kafka broker not reachable: {e}")
            exit(1)
        
        self.consumers = {}  # To track active consumers for each topic

    def create_topic(self, topic_name):
        # Kafka topics are created automatically upon first use.
        print(f"Kafka topic '{topic_name}' will be created upon first use.")

    def publish(self, topic_name, message, priority=1):
        # Add priority metadata to the message
        message_with_priority = {"priority": priority, "message": message}
        self.producer.send(topic_name, value=message_with_priority)
        print(f"Published message to Kafka topic '{topic_name}': {message_with_priority}")

    def subscribe(self, topic_name, callback, filter_func=None, group_id=None):
        def listen():
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            self.consumers[topic_name] = consumer
            print(f"Subscribed to Kafka topic '{topic_name}'")

            for message in consumer:
                msg_value = message.value
                if filter_func is None or filter_func(msg_value["message"]):
                    try:
                        callback(msg_value["message"])
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        self._save_undelivered_message(topic_name, msg_value["message"])

        threading.Thread(target=listen, daemon=True).start()

    def _save_undelivered_message(self, topic_name, message):
        # Save undelivered messages locally for replay
        file_name = f"undelivered_{topic_name}.json"
        with open(file_name, "a") as file:
            file.write(json.dumps({"message": message}) + "\n")
        print(f"Message saved to {file_name} for offline handling.")

    def replay_undelivered_messages(self, topic_name):
        file_name = f"undelivered_{topic_name}.json"
        if os.path.exists(file_name):
            print(f"Replaying undelivered messages for topic '{topic_name}'...")
            with open(file_name, "r") as file:
                for line in file:
                    try:
                        message_data = json.loads(line)
                        self.publish(topic_name, message_data["message"], priority=1)  # Replay with default priority
                    except json.JSONDecodeError as e:
                        print(f"Error reading undelivered message: {e}")
            os.remove(file_name)  # Remove file after replay
        else:
            print(f"No undelivered messages found for topic '{topic_name}'.")

# Example subscriber functions
def subscriber_one(message):
    print(f"Subscriber One received: {message}")

def subscriber_two(message):
    print(f"Subscriber Two received: {message}")

# Example filter: Only receive messages containing "Python"
def python_filter(message):
    return "Python" in message

# Example Usage
if __name__ == "__main__":
    kafka_manager = KafkaPubSubManager()

    # Create Topics
    kafka_manager.create_topic("news")
    kafka_manager.create_topic("sports")

    # Add Subscribers with and without filters
    kafka_manager.subscribe("news", subscriber_one)
    kafka_manager.subscribe("news", subscriber_two, python_filter, group_id="news_group")
    kafka_manager.subscribe("sports", subscriber_two, group_id="sports_group")

    # Publish Messages with Priority
    kafka_manager.publish("news", "Breaking News: Python is awesome!", priority=1)
    kafka_manager.publish("news", "General Update: AI is evolving!", priority=2)
    kafka_manager.publish("sports", "Live Update: Team A wins the match!", priority=1)

    # Simulate Replay of Undelivered Messages
    kafka_manager.replay_undelivered_messages("news")
