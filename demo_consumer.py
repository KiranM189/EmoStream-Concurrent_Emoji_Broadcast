from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'emoji-topic',  
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  
    enable_auto_commit=True,  
    group_id='emoji_consumer_group',  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

print("Consumer is listening to 'emoji_topic'...")

for message in consumer:
    emoji_data = message.value
    user_id = emoji_data.get("user_id")
    emoji_type = emoji_data.get("emoji_type")
    timestamp = emoji_data.get("timestamp")
    
    print(f"Received emoji data: User ID: {user_id}, Emoji: {emoji_type}, Timestamp: {timestamp}")