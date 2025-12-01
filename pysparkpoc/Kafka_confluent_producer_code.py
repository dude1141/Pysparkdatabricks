# Install the package before running:
# pip install confluent-kafka

import time
import random
from confluent_kafka import Producer

# Sample data
names = ['kala', 'ravi', 'sita', 'john', 'emma', 'alex', 'maya', 'liam', 'noah', 'olivia']
cities = ['del', 'mum', 'hyd', 'blr', 'nyc', 'ldn', 'par', 'tok', 'syd', 'dub']
confluent_key = r"VNH3POCG2IYDVGYP"
confluent_secret = r"cfltkmqEdneGzGwJJgJTmAYwqGG5ITHnRiXRN1w3mY+yCizLz5xVvYXQMPNDD97A"
topic = "nov27topic"
broker="pkc-921jm.us-east-2.aws.confluent.cloud:9092"

# Configuration for Confluent Kafka producer
conf = {
    'bootstrap.servers': broker,  # Example: 'pkc-xxxxxx.us-west1.gcp.confluent.cloud:9092'
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': confluent_key,
    'sasl.password': confluent_secret,
    'client.id': 'python-producer'
}

# Create Kafka producer instance
producer = Producer(conf)

# Function to handle delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Generate and send data
for _ in range(10000):
    name = random.choice(names)
    age = random.randint(18, 99)
    city = random.choice(cities)
    record = f"{name},{age},{city}"
    print(record)
    # Send record to Kafka topic
    producer.produce(topic, value=record.encode('utf-8'), callback=delivery_report)
    time.sleep(3)

# Wait for any outstanding messages to be delivered
producer.flush()

# Close the producer
producer.close()