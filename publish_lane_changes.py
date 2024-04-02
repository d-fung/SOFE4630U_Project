from google.cloud import pubsub_v1, bigquery
import json

# Initialize Publisher client
publisher = pubsub_v1.PublisherClient()

# Initialize BigQuery client
bigquery_client = bigquery.Client()

# Your Google Cloud project ID
project_id = "oceanic-granite-413404"

# Topics for lane changes
topic_laneChanges_none = f"projects/{project_id}/topics/laneChanges_none"
topic_laneChanges_occurred = f"projects/{project_id}/topics/laneChanges_occurred"

def publish_message(topic_name, data):
    """Publishes a message to a specified Pub/Sub topic."""
    # Data must be a bytestring
    data_str = json.dumps(data)
    data_bytes = data_str.encode("utf-8")
    
    # Publishes a message
    try:
        publish_future = publisher.publish(topic_name, data_bytes)
        publish_future.result()  # Waits for the publish call to complete.
        print(f"Message published to {topic_name}")
    except Exception as e:
        print(f"An error occurred: {e}")

def query_and_publish():
    # BigQuery SQL query
    query = """
    SELECT numLaneChanges, id, table_name FROM `HighD_meta_filtered.data`
    """
    try:
        query_job = bigquery_client.query(query)  # Make an API request, returns a QueryJob object
    except Exception as e:
        print(f"Failed to execute BigQuery query: {e}")

    for row in query_job:
        # Assuming 'numLaneChanges' is the column to check
        numLaneChanges = row["numLaneChanges"]
        id = row["id"]
        table_name = row["table_name"]
        
        data = {"id": id, "table_name": table_name, "numLaneChanges":numLaneChanges}
        
        if numLaneChanges == 0:
            publish_message(topic_laneChanges_none, data)
        else:
            publish_message(topic_laneChanges_occurred, data)

if __name__ == "__main__":
    query_and_publish()