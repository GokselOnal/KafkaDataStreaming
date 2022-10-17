from kafka import KafkaConsumer
import json

class Consumer:
    def __init__(self, topic_name):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest"
        )
        self.topic_name = topic_name

    def consume(self):
        for data in self.consumer: print(json.loads(data.value))