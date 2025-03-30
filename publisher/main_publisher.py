from kafka import KafkaConsumer, KafkaProducer
import json

def main_publisher():
    # Kafka consumer for the main publisher to receive aggregated emoji data
    consumer = KafkaConsumer(
        'emoji-aggregated',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        #auto_offset_reset='earliest',
        #enable_auto_commit=True,
        #group_id='main-publisher-group'  # Main Publisher Consumer Group
    )

    # Kafka producer to forward messages to each cluster's publisher
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("Main Publisher started consuming from 'emoji_aggregated' topic...")

    for message in consumer:
        emoji_data = message.value
        emoji_type = emoji_data['emoji_type']
        print(message.value)
        producer.send('cluster_1', emoji_data)

if __name__ == '__main__':
    main_publisher()
