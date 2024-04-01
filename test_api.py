
import requests

# Define the URL of the API endpoint
url = "http://127.0.0.1:8000/create-video/"

# Define the JSON payload to send to the API
data = {
    "table_name": "01_tracks",
    "vehicle_id": 1,
    "numLaneChanges": 1
}

# Send a POST request to the URL with the JSON data
response = requests.post(url, json=data)

# Print the response from the server
print(f"Status Code: {response.status_code}")
print(f"Response Body: {response.json()}")
