import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.animation import FFMpegWriter
import pandas as pd
from PIL import Image
import io
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio

app = FastAPI()

class VideoRequest(BaseModel):
    table_name: str
    vehicle_id: int
    numLaneChanges: int

from google.cloud import pubsub_v1, bigquery, storage

bigquery_client = bigquery.Client()
storage_client = storage.Client()

bucket = storage_client.bucket('oceanic-granite-413404-bucket')

# Queries the HighD_tracks database to return all the frames that the vehicle ID is in
def query_database(table_name, vehicle_id):
    query = f"""
    SELECT frame, id, x, y, width, height FROM `oceanic-granite-413404.HighD_tracks.{table_name}`
    WHERE frame BETWEEN (
        SELECT MIN(frame)
        FROM `oceanic-granite-413404.HighD_tracks.{table_name}`
        WHERE id = {vehicle_id}
    ) AND (
        SELECT MAX(frame)
        FROM `oceanic-granite-413404.HighD_tracks.{table_name}`
        WHERE id = {vehicle_id}
    );
    """
    try:
        query_job = bigquery_client.query(query)
        results = query_job.result()
        df = results.to_dataframe()
        return df # return as a df for processing
    except Exception as e:
        print(f"Failed to execute BigQuery query: {e}")
        return None

# Parses the table name for the numerical ID
def get_table_id(table_name):
    parts = table_name.split("_")
    try:
        table_id = int(parts[0])
        return table_id
    except ValueError as e:
        print(f"Failed to convert table name to int: {e}")
        return None
    
# Queries the HighD_recording_meta dataset to find the lane markings to be used in the video
def query_recording_meta(table_id):
        
    query = f"""
    SELECT upperLaneMarkings, lowerLaneMarkings FROM `oceanic-granite-413404.HighD_recording_meta.data`
    WHERE id = {table_id};
    """

    try:
        query_job = bigquery_client.query(query)
        lane_markings_data = query_job.result().to_dataframe()  # Convert the query job results to a DataFrame
        upper_lane_markings = list(map(float, lane_markings_data['upperLaneMarkings'].iloc[0].split(';')))
        lower_lane_markings = list(map(float, lane_markings_data['lowerLaneMarkings'].iloc[0].split(';')))

        return upper_lane_markings, lower_lane_markings
    
    except Exception as e:
        print(f'Failed to execute BigQuery query: {e}')
        return None

# Opens the background image corresponding to the table_id for the video background
def open_image_from_gcs(table_id):
    if table_id < 10 and table_id > 0:
        table_id = f'0{table_id}'

    blob = bucket.blob(f'data/{table_id}_highway.png')
    #blob = bucket.blob(f'data/04_highway.png')
    try:
        image_bytes = blob.download_as_bytes()
        image = Image.open(io.BytesIO(image_bytes))

        # Get the original dimensions of the image
        original_width, original_height = image.size

        # Crop the image to its left half
        left_half = (0, 0, original_width / 2, original_height)
        cropped_image = image.crop(left_half)

        return cropped_image


    except Exception as e:
        print(f'Error in opening image from gcs: {e}')
        return None


def plot_highway(ax, highway_image, upper_lane_markings, lower_lane_markings, lane_color='white', outer_line_thickness=0.5):    
    image_width = highway_image.size[0]
    
    # Calculate the top and bottom lane area
    top_lane_area = upper_lane_markings[0]
    bottom_lane_area = lower_lane_markings[-1]
    
    # Top lane area
    rect = plt.Rectangle((0, top_lane_area), image_width, bottom_lane_area - top_lane_area,
                         color="grey", fill=True, alpha=1, zorder=5)
    ax.add_patch(rect)
    
    # Upper lane markings
    for i, y in enumerate(upper_lane_markings):
        rect = plt.Rectangle((0, y), image_width, outer_line_thickness,
                             color=lane_color, fill=True, alpha=1, zorder=5)
        ax.add_patch(rect)
    
    # Lower lane markings
    for i, y in enumerate(lower_lane_markings):
        rect = plt.Rectangle((0, y), image_width, outer_line_thickness,
                             color=lane_color, fill=True, alpha=1, zorder=5)
        ax.add_patch(rect)

    # Set the y-limits based on the lane markings
    ax.set_ylim(bottom_lane_area + 5, top_lane_area - 5)

def update_plot(frame_number, track_data, vehicle_id, ax, highway_image, fig, upper_lane_markings, lower_lane_markings):
    ax.clear()
    ax.imshow(highway_image, extent=[0, highway_image.size[0], 0, highway_image.size[1]])
    ax.axis('off')
    plot_highway(ax, highway_image, upper_lane_markings, lower_lane_markings)
    
    # Filter the data for the given frame number
    frame_data = track_data[track_data['frame'] == frame_number]
    
    for index, vehicle in frame_data.iterrows():
        # Use the vehicle's width as the dimension in the direction of movement
        # and height as the perpendicular dimension to the direction of movement
        vehicle_width = vehicle['width']
        vehicle_height = vehicle['height']
        
        # Assuming the 'x' and 'y' positions mark the center of the vehicle
        # Calculate bottom-left coordinates for drawing the rectangle
        bottom_left_x = vehicle['x'] - vehicle_width / 2
        bottom_left_y = vehicle['y'] - vehicle_height / 2
        
        # Draw the vehicle rectangle with the width along the direction of movement
        if vehicle['id'] == vehicle_id: # Subject vehicle is red
            rect = plt.Rectangle((bottom_left_x, bottom_left_y), vehicle_width, vehicle_height,
                             color='red', fill=True, zorder=10)  # Ensure zorder is high enough to be on top
            ax.add_patch(rect)
        else:
            rect = plt.Rectangle((bottom_left_x, bottom_left_y), vehicle_width, vehicle_height,
                                color='blue', fill=True, zorder=10)  # Ensure zorder is high enough to be on top
            ax.add_patch(rect)

        # Optionally, add text labels for vehicle IDs, positioned at the center of the vehicle
        ax.text(vehicle['x'], vehicle['y'], str(vehicle['id']), color='white', fontsize=8, zorder=11)
        

def create_video(table_name, vehicle_id, numLaneChanges):
    table_id = get_table_id(table_name)
    highway_image = open_image_from_gcs(table_id)
    
    scenario_data = query_database(table_name, vehicle_id) # should be a dataframe object
    upper_lane_markings, lower_lane_markings = query_recording_meta(table_id)

    fig, ax = plt.subplots(figsize=(15, 10))

    # Animate plot with update_plot function and pass the lane markings
    ani = animation.FuncAnimation(fig, update_plot, frames=sorted(scenario_data['frame'].unique()), 
                                fargs=(scenario_data, vehicle_id, ax, highway_image, fig, upper_lane_markings, lower_lane_markings), interval=100)

    
    # Assign folder name corresponding to scenario type
    if numLaneChanges > 0:
        folder_name = 'videos/lane_changes_occurred'
    else:
        folder_name = 'videos/lane_changes_none'

    video_file_name = f'{table_id}-{vehicle_id}_scenario.mp4'


    print('processing video')
    writer = FFMpegWriter(fps=10, metadata=dict(artist='Me'), bitrate=1800)
    ani.save(video_file_name, writer=writer)

    # Uploads video to the folder
    destination_blob_name = f'{folder_name}/{video_file_name}'

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(video_file_name)

    print('done')
    os.remove(video_file_name)



@app.post("/create-video/")
def create_video_api(video_request: VideoRequest):
    try:
        create_video(video_request.table_name, video_request.vehicle_id, video_request.numLaneChanges)
        
        return {"message": "Video processing finished", "table_name": video_request.table_name, "vehicle_id": video_request.vehicle_id, "numLaneChanges": video_request.numLaneChanges}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

