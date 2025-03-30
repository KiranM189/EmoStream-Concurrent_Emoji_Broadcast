import requests
import sseclient
import json
from datetime import datetime

def register_client():
    response = requests.get('http://localhost:5000/register')
    if response.status_code == 200:
        server_info = response.json()
        server_url = server_info.get("server_url")
        server_name = server_info.get("server_name")
        user_id = server_info.get("user_id")
        print(f"Assigned to server: {server_name}")
        return server_url,server_name,user_id
    else:
        print("Failed to register client")
        return None

def notify_server_leave(server_name):
    data = {"server": server_name}
    response = requests.post('http://localhost:5000/unregister', json=data)
    if response.status_code == 200:
        print("Successfully notified the server of disconnection.")
    else:
        print("Failed to notify server of disconnection.")

def consume_sse():
    while True:
        try:
            server_url,server_name,user_id = register_client()
            url = "http://localhost:5000/send_emoji"
            data = {
                "user_id": user_id,
                "emoji_type": '😠'
            }
            data["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            response = requests.post(url, json=data)
            print(data['emoji_type'],datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
            SSE_URL = server_url
            if SSE_URL:
                response = requests.get(SSE_URL, stream=True)
                client = sseclient.SSEClient(response)
                for event in client.events():
                    try:
                        emoji_data = json.loads(event.data)
                        print(f"Received emoji: {emoji_data}")
                    except json.JSONDecodeError:
                        print(f"Failed to decode: {event.data}")
            else:
                print("Failed to get a server URL, retrying registration...")
        except KeyboardInterrupt:
            notify_server_leave(server_name)
            break


if __name__ == '__main__':
    consume_sse()
