from kafka import KafkaConsumer

def kafka_consumer():
    consumer = KafkaConsumer(
        'emoji-aggregated',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest',  # Start from earliest if there are no committed offsets
        enable_auto_commit=True,       # Auto commit the offsets
        group_id='emoji-consumer-group'  # Set consumer group to manage offsets
    )

    print("Started consuming from 'emoji-aggregated' topic...")
    for message in consumer:
        print(f"Received message: {message.value}")

if __name__ == '__main__':
    kafka_consumer()
