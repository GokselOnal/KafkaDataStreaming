from client import Client
from producer import Producer
from consumer import Consumer


if __name__ == '__main__':
    Client().create_topic(name="api_data", partition=2, replication=1)
    Producer(topic_name="api_data").produce()
    Consumer(topic_name="api_data").consume()
