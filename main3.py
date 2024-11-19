from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500  # Flush interval of 500 milliseconds
)
@app.route('/register',method=['POST'])
def handle_registration:
    
@app.route('/send_emoji', methods=['POST'])
def handle_emoji():
    data = request.get_json()
    user_id = data.get('user_id')
    emoji_type = data.get('emoji_type')
    timestamp = data.get('timestamp')
    msg = {
        "user_id": user_id,
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }

    producer.send("emoji-topic", msg)  # Add message to producer's buffer
    return jsonify({"status": "Message queued"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
