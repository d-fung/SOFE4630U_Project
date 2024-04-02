from google.cloud import pubsub_v1
import json
import requests

project_id = "oceanic-granite-413404"
subscription_id = "laneChanges_occurred-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

fastapi_url = '"http://127.0.0.1:8000/create-video/"'

# Callback function will call the visualization API service

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    print(f"Data: {data}")
    # Send a POST request to the FastAPI application
    try:
        response = requests.post(fastapi_url, json=data)
        if response.status_code == 200:
            print("Request to FastAPI successful.")
        else:
            print("Failed to send data to FastAPI.")
    except Exception as e:
        print(f"Error sending data to FastAPI: {e}")    
        
    message.ack()

# Main function to keep listening for messages
def listen_for_messages():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    # Keep the main thread alive, or the subscriber will stop listening
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

if __name__ == "__main__":
    listen_for_messages()