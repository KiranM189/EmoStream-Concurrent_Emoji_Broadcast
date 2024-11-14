from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import time
import asyncio

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

async def send_to_kafka(topic, message):
    producer.send(topic, message)
    await asyncio.sleep(0.5)
    producer.flush()


@app.route('/send_emoji', methods=['POST'])
async def handle_emoji():
        data = request.get_json()  # Get the JSON data from the request
        user_id = data.get('user_id')
        emoji_type = data.get('emoji_type')
        timestamp = data.get('timestamp')
        msg = {
            "user_id": user_id,
            "emoji_type": emoji_type,
            "timestamp": timestamp
        }

        await send_to_kafka("emoji-topic", msg)
        return jsonify({"status": "Message queued"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)