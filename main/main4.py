from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500  
)

user_count = 0

port = {
    "sub1":5050,
    "sub2":5500,
    "sub3":5003,
    "sub4":5004,
    "sub5":5005,
    "sub6":5006
}
server_client_count = {
    "sub1": 0,
    "sub2": 0,
    "sub3": 0,
    "sub4": 0,
    "sub5": 0,
    "sub6": 0
}

@app.route('/register', methods=['GET'])
def handle_registration():
    global user_count
    user_count+=1
    server_to_assign = min(server_client_count, key=server_client_count.get)
    server_client_count[server_to_assign] += 1
    if server_client_count[server_to_assign] > 1000:
        return jsonify({"message":"Too many Clients"}), 500
    else:
        server_url = f'http://localhost:{port[server_to_assign]}/{server_to_assign}'
        return jsonify({"server_url": server_url, "server_name":server_to_assign, "user_id":f"user_{str(user_count)}", "message": f"Assigned to {server_to_assign}"}), 200

@app.route('/unregister', methods=['POST'])
def handle_client_leave():
    data = request.get_json()
    server = data.get('server')  
    if server and server in server_client_count:
        server_client_count[server] = max(0, server_client_count[server] - 1)
        return jsonify({"message": f"Decreased client count for {server}"}), 200
    else:
        return jsonify({"error": "Invalid server name"}), 400

@app.route('/send_emoji', methods=['POST'])
def handle_emoji():
    data = request.get_json()
    user_id = data.get('user_id')
    emoji_type = data.get('emoji_type')
    timestamp = data.get('timestamp')
    
    # Create the message
    msg = {
        "user_id": user_id,
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }
    
    # Send the message to the Kafka topic
    producer.send("emoji-topic", msg)  # Add message to producer's buffer
    return jsonify({"status": "Message queued"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True,port=5000)
