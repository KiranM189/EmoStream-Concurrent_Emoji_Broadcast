from flask import Flask, Response
from kafka import KafkaConsumer
import json
import threading

app = Flask(__name__)

def create_consumer():
    return KafkaConsumer(
        'sub_1',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',  # This can be modified as needed
        enable_auto_commit=True,
        group_id=None  # No group ID ensures each consumer gets its own copy of messages
    )

# Generator function for sending emojis to clients
def send_emoji():
    consumer = create_consumer()  # Create a new consumer for each client
    for msg in consumer:
        print(msg.value)
        yield f"data: {json.dumps(msg.value)}\n\n"  # Formatting as Server-Sent Events (SSE)

@app.route('/sub1')
def stream():
    return Response(send_emoji(), content_type='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True, port=5050)
