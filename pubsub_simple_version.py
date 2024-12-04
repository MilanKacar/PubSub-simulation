import threading
import queue
import json
import os


class PubSubManager:
    def __init__(self):
        self.topics = {}  # Dictionary to hold topics and their subscribers
        self.locks = {}  # Locks for each topic to ensure thread-safety

    def create_topic(self, topic_name):
        with threading.Lock():  # Ensure thread-safe topic creation
            if topic_name not in self.topics:
                self.topics[topic_name] = {
                    "subscribers": [],
                    "queue": queue.PriorityQueue(),  # PriorityQueue for prioritized delivery
                }
                self.locks[topic_name] = threading.Lock()
                print(f"Topic '{topic_name}' created.")
            else:
                print(f"Topic '{topic_name}' already exists.")

    def subscribe(self, topic_name, callback, filter_func=None):
        if topic_name in self.topics:
            with self.locks[topic_name]:  # Lock topic during subscription
                self.topics[topic_name]["subscribers"].append({"callback": callback, "filter": filter_func})
                print(f"Subscriber added to topic '{topic_name}'.")
        else:
            print(f"Topic '{topic_name}' does not exist. Cannot subscribe.")

    def publish(self, topic_name, message, priority=1):
        if topic_name in self.topics:
            print(f"Publishing message to topic '{topic_name}': {message} with priority {priority}")
            with self.locks[topic_name]:  # Lock topic during publish
                self.topics[topic_name]["queue"].put((priority, message))
            threading.Thread(target=self._deliver_messages, args=(topic_name,)).start()
        else:
            print(f"Topic '{topic_name}' does not exist.")

    def _deliver_messages(self, topic_name):
        while not self.topics[topic_name]["queue"].empty():
            _, message = self.topics[topic_name]["queue"].get()
            with self.locks[topic_name]:  # Lock topic during delivery
                for subscriber in self.topics[topic_name]["subscribers"]:
                    # Apply filter if specified
                    if subscriber["filter"] is None or subscriber["filter"](message):
                        try:
                            subscriber["callback"](message)
                        except Exception as e:
                            print(f"Error delivering message: {e}")
                            self._save_undelivered_message(topic_name, message)

    def _save_undelivered_message(self, topic_name, message):
        # Save undelivered messages to a file for persistence
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


# Example subscribers
def subscriber_one(message):
    print(f"Subscriber One received: {message}")


def subscriber_two(message):
    print(f"Subscriber Two received: {message}")


# Example filter: Only receive messages containing "Python"
def python_filter(message):
    return "Python" in message


# Example Usage
if __name__ == "__main__":
    # Initialize Pub/Sub Manager
    manager = PubSubManager()

    # Create Topics
    manager.create_topic("news")
    manager.create_topic("sports")

    # Add Subscribers with and without filters
    manager.subscribe("news", subscriber_one)
    manager.subscribe("news", subscriber_two, python_filter)
    manager.subscribe("sports", subscriber_two)

    # Publish Messages with Priority
    manager.publish("news", "Breaking News: Python is awesome!", priority=1)
    manager.publish("news", "General Update: AI is evolving!", priority=2)
    manager.publish("sports", "Live Update: Team A wins the match!", priority=1)

    # Simulate Replay of Undelivered Messages
    manager.replay_undelivered_messages("news")
