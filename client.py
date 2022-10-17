from kafka import KafkaAdminClient
from kafka.admin import NewTopic

class Client:
    def __init__(self):
        self.admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='kafka_')
        self.topic_list = list()

    def create_topic(self, name, partition, replication):
        new_topic = NewTopic(name=name, num_partitions=partition, replication_factor=replication)
        self.topic_list.append(new_topic)
        self.admin_client.create_topics(new_topics= self.topic_list)