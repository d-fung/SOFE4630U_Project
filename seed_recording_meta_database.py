import subprocess
import os
import re

bucket_name = 'oceanic-granite-413404-bucket/data'
bigquery_dataset = 'HighD_recording_meta'
bigquery_table = 'data'
project_id = 'oceanic-granite-413404'

# Command to list all CSV files in the bucket
cmd_list_files = f'gsutil ls gs://{bucket_name}/*.csv'

try:
    # Execute the command and capture the output
    csv_files = subprocess.check_output(cmd_list_files, shell=True, text=True).splitlines()
    pattern = re.compile(r'^.*/(\d{2}_recordingMeta)\.csv$')

    for file_path in csv_files:
        match = pattern.match(file_path)
        if match:
            # Construct the BigQuery load command for the single table
            cmd_load = (
                f'bq load --source_format=CSV --autodetect '
                f'--field_delimiter=, '
                f'--project_id={project_id} '
                f'--replace=false '  # Ensure this is false to append data
                f'{project_id}:{bigquery_dataset}.{bigquery_table} '
                f'{file_path}'
            )
            # Execute the load command
            subprocess.run(cmd_load, shell=True, check=True)
            print(f"Loaded {file_path} into BigQuery table {bigquery_dataset}.{bigquery_table}")
except subprocess.CalledProcessError as e:
    print(f"Failed to execute command: {e}")
