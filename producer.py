from secret import SecretFile
from kafka import KafkaProducer
import requests
import json

class Producer:
    def __init__(self, topic_name):
        self.producer = KafkaProducer(bootstrap_servers="localhost:9092",
                                      value_serializer= self.serializer
                        )
        self.topic_name = topic_name

    def serializer(self, data):
        return json.dumps(data).encode('utf-8')

    def data_get(self):
        api_url = "https://api.api-ninjas.com/v1/cars?year=2020&limit=30"
        response = requests.get(api_url, headers={'X-Api-Key': SecretFile.api_key})
        if response.status_code == requests.codes.ok: return response.json()
        else: print("Error:", response.status_code, response.text)

    def produce(self):
        data = self.data_get()
        for d in data: self.producer.send(self.topic_name, d)