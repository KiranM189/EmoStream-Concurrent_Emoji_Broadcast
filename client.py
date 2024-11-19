import requests
import json
import random
from datetime import datetime

url = "http://localhost:5000/send_emoji"
array=['😀','😭','😠','💖','😲','🔥','😂']
# Send a POST request with the JSON payload
for i in range(100):
    data = {
        "user_id": "user123",
        "emoji_type": array[random.randint(0, 6)]
    }
    data["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    response = requests.post(url, json=data)
    
    print(data['emoji_type'],datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
