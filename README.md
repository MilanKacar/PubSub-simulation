# Pub/Sub Simulation

This project demonstrates two implementations of a Publish-Subscribe (Pub/Sub) messaging system. It showcases how publishers, subscribers, and a broker interact, with features like prioritized message delivery and undelivered message handling.

## Features
- **Publishers:** Send messages to topics.
- **Subscribers:** Receive messages based on topic subscriptions.
- **Broker:** Routes messages between publishers and subscribers.
- **Prioritized Delivery:** Ensures high-priority messages are delivered first.
- **Undelivered Message Handling:** Saves messages locally if delivery fails and supports replay.

## Files
| File Name                 | Description                                                                                       |
|---------------------------|---------------------------------------------------------------------------------------------------|
| `pubsub_kafka.py`         | Implements Pub/Sub using Kafka as the underlying broker for scalable and distributed messaging.   |
| `pubsub_simple_version.py`| A simple in-memory Pub/Sub implementation using Python threads and queues for lightweight testing.|

## Getting Started

### Prerequisites
- **For Kafka Version:**
  - Kafka installed and running locally or remotely.
  - Python dependencies (install via `pip install kafka-python`).

- **For Simple Version:**
  - Python 3.x.
  - No external dependencies required.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/PubSub-simulation.git
   cd PubSub-simulation
   ```

2. Install dependencies:
   - For Kafka:
     ```bash
     pip install kafka-python
     ```

### Usage

#### Kafka Version
1. Ensure Kafka is running on the default port (`localhost:9092`).
2. Run the script:
   ```bash
   python pubsub_kafka.py
   ```

#### Simple Version
1. Run the script directly:
   ```bash
   python pubsub_simple_version.py
   ```

### Example Workflow
1. Create topics like `news` or `sports`.
2. Add subscribers to topics, with optional filters (e.g., only messages containing "Python").
3. Publish messages to topics with priorities (e.g., priority 1 for urgent).
4. Handle undelivered messages using the replay feature.

### Key Classes and Functions

#### `pubsub_kafka.py`
- **`KafkaPubSubManager`**: Manages topics, publishers, and subscribers using Kafka.
- **`publish(topic, message, priority)`**: Publishes messages with a priority.
- **`subscribe(topic, callback, filter_func)`**: Subscribes to a topic with optional filtering.

#### `pubsub_simple_version.py`
- **`PubSubManager`**: Manages topics, publishers, and subscribers using an in-memory queue.
- **`publish(topic, message, priority)`**: Publishes messages to a topic.
- **`subscribe(topic, callback, filter_func)`**: Subscribes to a topic with optional filters.

## Future Enhancements
- Add support for advanced message filtering and acknowledgment.
- Integrate persistent storage for the simple version.
- Support scaling for the in-memory version using distributed systems.

## License
This project is open-source and available under the MIT License

## Contact
For questions or contributions, feel free to contact:
- **Author:** Milan Kacar
- **GitHub:** [https://github.com/MilanKacar](https://github.com/MilanKacar)
