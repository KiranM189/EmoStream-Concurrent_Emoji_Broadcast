from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import math

def cluster_publisher():
    consumer = KafkaConsumer(
        'cluster_1',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        #auto_offset_reset='earliest',
        #enable_auto_commit=True,
        #group_id=f'cluster-{cluster_id}-group'  # Unique group for each cluster
    )

    print(f"Cluster Publisher started consuming from 'cluster_topic'...")

    for message in consumer:
        emoji_data = message.value
        for i in range(0,int(math.ceil(emoji_data["emoji_count_aggregated"]))):
            emoji_type = emoji_data['emoji_type']
            print(f"Cluster Publisher received emoji_type: {emoji_type}")
            forward_to_subscribers(emoji_type)

def forward_to_subscribers(emoji_data):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('sub_2',emoji_data)

if __name__ == '__main__':
    cluster_publisher()
