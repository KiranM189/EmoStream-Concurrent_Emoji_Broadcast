import requests
import sseclient
import json

SSE_URL = 'http://localhost:5050/sub1'

def consume_sse():
    """Consumes Server-Sent Events from the Flask SSE server."""
    while True:
        response = requests.get(SSE_URL, stream=True)
        client = sseclient.SSEClient(response)
        for event in client.events():
            try:
                # Parse the event data and load it as a JSON object
                emoji_data = json.loads(event.data)
                # Print the decoded emoji
                print(f"Received emoji: {emoji_data}")
            except json.JSONDecodeError:
                print(f"Failed to decode: {event.data}")

if __name__ == '__main__':
    consume_sse()
