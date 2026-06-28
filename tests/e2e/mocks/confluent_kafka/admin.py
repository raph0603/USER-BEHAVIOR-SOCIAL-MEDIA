import os

class NewTopic:
    def __init__(self, topic, num_partitions=1, replication_factor=1):
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor

class MockFuture:
    def __init__(self, topic):
        self.topic = topic

    def result(self, timeout=None):
        if os.getenv("MOCK_ADMIN_CREATE_TOPICS_FAILURE") == "true":
            raise RuntimeError("Simulated Kafka topic creation failure")
        return None

class AdminClient:
    def __init__(self, config):
        self.config = config

    def create_topics(self, new_topics, **kwargs):
        futures = {}
        for new_topic in new_topics:
            # support both NewTopic objects and raw strings if any
            topic_name = new_topic.topic if isinstance(new_topic, NewTopic) else str(new_topic)
            futures[topic_name] = MockFuture(topic_name)
        return futures
