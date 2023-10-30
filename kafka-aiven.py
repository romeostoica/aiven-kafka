from confluent_kafka import Producer
import json
import uuid
from datetime import datetime
from faker import Faker

# Define the Kafka broker and topic details
bootstrap_servers = "kafka-275297e7-romeo-f2fe.a.aivencloud.com:21359"
topic = "iottopic"

# Create a Kafka producer instance
producer_config = {
    'bootstrap.servers':bootstrap_servers,
    'security.protocol':'SSL',
    'ssl.ca.location':'ca.pem',
    'ssl.certificate.location':'service.cert',
    'ssl.key.location':'service.key'
}

producer = Producer(producer_config)

# Function to generate a random event in JSON format
def generate_random_event():
    fake = Faker()
    event_id = fake.uuid4()
    timestamp = fake.date_time_this_decade(tzinfo=None).isoformat()
    temperature = fake.random_int(min=0, max=40)
    humidity = fake.random_int(min=30, max=100)
    event_data = {
        "event_id": event_id,
        "timestamp": timestamp,
	"temperature": temperature,
	"humidity": humidity
    }
    return json.dumps(event_data)

# Produce JSON data to the Kafka topic
fake = Faker()
event_data = generate_random_event()
producer.produce(topic, key=str(fake.uuid4()), value=event_data)
print(f"Produced message: {event_data}")

producer.flush()


