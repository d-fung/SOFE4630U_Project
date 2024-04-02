import subprocess
import os
import re

bucket_name = 'oceanic-granite-413404-bucket/data'
bigquery_dataset = 'HighD_meta'  
project_id = 'oceanic-granite-413404'

# Command to list all CSV files in the bucket
cmd_list_files = f'gsutil ls gs://{bucket_name}/*.csv'

try:
    # Execute the command and capture the output
    csv_files = subprocess.check_output(cmd_list_files, shell=True, text=True).splitlines()

    # Define a regex pattern to match files
    pattern = re.compile(r'^.*/(\d{2}_tracksMeta)\.csv$')

    for file_path in csv_files:
        match = pattern.match(file_path)
        if match:
            # Extract table name from the regex group
            table_name = match.group(1)
            # Construct the BigQuery load command
            # Here, explicitly specify the project ID with the dataset
            cmd_load = (
                f'bq load --source_format=CSV --autodetect '
                f'--field_delimiter=, '
                f'--project_id={project_id} '
                f'{project_id}:{bigquery_dataset}.{table_name} '
                f'{file_path}'
            )
            # Execute the load command
            subprocess.run(cmd_load, shell=True, check=True)
            print(f"Loaded {file_path} into BigQuery table {bigquery_dataset}.{table_name}")
except subprocess.CalledProcessError as e:
    print(f"Failed to execute command: {e}")