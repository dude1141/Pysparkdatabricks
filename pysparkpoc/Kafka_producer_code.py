import time
import random
from kafka import KafkaProducer

# Sample data
names = ['kala', 'ravi', 'sita', 'john', 'emma', 'alex', 'maya', 'liam', 'noah', 'olivia']
cities = ['del', 'mum', 'hyd', 'blr', 'nyc', 'ldn', 'par', 'tok', 'syd', 'dub']

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for _ in range(10000):
    name = random.choice(names)
    age = random.randint(18, 99)
    city = random.choice(cities)
    record = f"{name},{age},{city}"
    print(record)
    producer.send('nov20', value=record.encode('utf-8'))
    time.sleep(3)

producer.close()