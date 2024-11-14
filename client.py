import requests
import json
from datetime import datetime

url = "http://localhost:5000/send_emoji"

# Send a POST request with the JSON payload
for i in range(20):
    data = {
        "user_id": "user123",
        "emoji_type": "happy",
    }
    data["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    response = requests.post(url, json=data)
    
    print(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
