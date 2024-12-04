import unittest
import os
from unittest.mock import MagicMock
from pubsub_simple_version import PubSubManager  # Replace with the actual module name


class TestPubSubManager(unittest.TestCase):

    def setUp(self):
        """Set up a new instance of PubSubManager for each test."""
        self.manager = PubSubManager()

    def tearDown(self):
        """Clean up any undelivered message files created during tests."""
        for topic in ["test_topic", "news", "sports"]:
            file_name = f"undelivered_{topic}.json"
            if os.path.exists(file_name):
                os.remove(file_name)

    def test_create_topic(self):
        """Test topic creation and ensure duplicate creation is handled."""
        self.manager.create_topic("test_topic")
        self.assertIn("test_topic", self.manager.topics)
        self.assertIn("test_topic", self.manager.locks)

        # Attempt to create the same topic again
        self.manager.create_topic("test_topic")
        self.assertEqual(len(self.manager.topics), 1)

    def test_subscribe(self):
        """Test subscriber registration with and without filters."""
        self.manager.create_topic("test_topic")

        callback_mock = MagicMock()
        self.manager.subscribe("test_topic", callback_mock)

        # Ensure the subscriber is added
        self.assertEqual(len(self.manager.topics["test_topic"]["subscribers"]), 1)

        # Subscribe with a filter
        filter_mock = MagicMock(return_value=True)
        self.manager.subscribe("test_topic", callback_mock, filter_func=filter_mock)
        self.assertEqual(len(self.manager.topics["test_topic"]["subscribers"]), 2)

    def test_publish_and_deliver(self):
        """Test publishing and delivering messages."""
        self.manager.create_topic("test_topic")

        callback_mock = MagicMock()
        self.manager.subscribe("test_topic", callback_mock)

        self.manager.publish("test_topic", "Hello, World!", priority=1)

        # Allow time for delivery
        self.manager._deliver_messages("test_topic")
        callback_mock.assert_called_once_with("Hello, World!")

    def test_filter_function(self):
        """Test message filtering for subscribers."""
        self.manager.create_topic("test_topic")

        callback_mock = MagicMock()
        filter_mock = MagicMock(return_value=False)  # Reject all messages
        self.manager.subscribe("test_topic", callback_mock, filter_func=filter_mock)

        self.manager.publish("test_topic", "Hello, World!", priority=1)
        self.manager._deliver_messages("test_topic")

        # Callback should not be called due to the filter
        callback_mock.assert_not_called()
        filter_mock.assert_called_once_with("Hello, World!")

    def tearDown(self):
        # Clean up any undelivered message files created during tests
        file_name = "undelivered_test_topic.json"
        if os.path.exists(file_name):
            os.remove(file_name)

    def _save_undelivered_message(self, topic_name, message):
        file_name = f"undelivered_{topic_name}.json"
        try:
            with open(file_name, "a") as file:
                file.write(json.dumps({"message": message}) + "\n")
                file.flush()  # Ensure data is written immediately
        except Exception as e:
            print(f"Error writing undelivered message to file: {e}")
        print(f"Message saved to {file_name} for offline handling.")

    def test_undelivered_messages(self):
        """Test handling of undelivered messages due to subscriber failure."""
        self.manager.create_topic("test_topic")

        def failing_callback(message):
            raise Exception("Delivery failed")

        self.manager.subscribe("test_topic", failing_callback)
        self.manager.publish("test_topic", "Undeliverable Message", priority=1)

        # Allow time for delivery attempt
        self.manager._deliver_messages("test_topic")

        # Debug print to check if the file exists
        file_name = "undelivered_test_topic.json"
        print("Checking if file exists:", os.path.exists(file_name))

        # Verify undelivered message file
        self.assertTrue(os.path.exists(file_name))


    def test_replay_undelivered_messages(self):
        """Test replaying undelivered messages."""
        self.manager.create_topic("test_topic")

        # Save a message to the undelivered file manually
        file_name = "undelivered_test_topic.json"
        with open(file_name, "w") as file:
            file.write('{"message": "Replayed Message"}\n')

        callback_mock = MagicMock()
        self.manager.subscribe("test_topic", callback_mock)

        # Replay undelivered messages
        self.manager.replay_undelivered_messages("test_topic")

        # Ensure the callback was called with the replayed message
        callback_mock.assert_called_once_with("Replayed Message")

        # Verify the file is deleted after replay
        self.assertFalse(os.path.exists(file_name))

if __name__ == "__main__":
    unittest.main()
