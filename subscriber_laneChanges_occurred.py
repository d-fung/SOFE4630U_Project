from google.cloud import pubsub_v1
import json
import requests

project_id = "oceanic-granite-413404"
subscription_id = "laneChanges_occurred-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

fastapi_url = 'http://your-fastapi-service-url/create-video/'


def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    print(f"Data: {data}")
    # Send a POST request to your FastAPI application
    try:
        response = requests.post(fastapi_url, json=data)
        if response.status_code == 200:
            print("Request to FastAPI successful.")
        else:
            print("Failed to send data to FastAPI.")
    except Exception as e:
        print(f"Error sending data to FastAPI: {e}")    
        
    message.ack()

def listen_for_messages():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    # Keep the main thread alive, or the subscriber will stop listening
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()  # Trigger the shutdown
        streaming_pull_future.result()  # Block until the shutdown is complete

if __name__ == "__main__":
    listen_for_messages()