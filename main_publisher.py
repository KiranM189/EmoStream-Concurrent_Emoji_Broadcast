from kafka import KafkaConsumer

def kafka_consumer():
    consumer = KafkaConsumer(
        'emoji-aggregated',                
        bootstrap_servers=['ed-kafka:29092'],  
        auto_offset_reset='earliest',      
        enable_auto_commit=True,           
        group_id='emoji-aggregated-consumer-group',  
        value_deserializer=lambda x: x.decode('utf-8')  
    )

    print("Started consuming from 'emoji-aggregated' topic...")

    for message in consumer:
        print(f"Received message: {message.value}")

if __name__ == '__main__':
    kafka_consumer()
