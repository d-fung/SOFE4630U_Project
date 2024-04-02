import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from google.cloud import bigquery

def construct_union_query(dataset_id, project_id):
    client = bigquery.Client(project=project_id)
    tables = client.list_tables(dataset_id)
    
    query_parts = []
    for table in tables:
        table_name = table.table_id
        query_part = f'SELECT *, "{table_name}" as table_name FROM `{project_id}.{dataset_id}.{table_name}` WHERE minTTC < 2 AND minTTC > 0'
        query_parts.append(query_part)
    
    union_query = " UNION ALL ".join(query_parts)
    return union_query


def run_union_pipeline(project_id, dataset_id, output_table_id, pipeline_args):
    union_query = construct_union_query(dataset_id, project_id)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--region=northamerica-northeast2',
        '--job_name=highd-meta-filtering-pipeline',
    ])
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    
    with beam.Pipeline(options=pipeline_options) as p:
        filtered_rows = (p 
                         | 'Read Unioned Tables' >> ReadFromBigQuery(
                             query=union_query, 
                             use_standard_sql=True,
                             project=project_id)
                        )
        
        filtered_rows | 'Write to BigQuery' >> WriteToBigQuery(
            output_table_id,
            schema='SCHEMA_AUTODETECT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    PROJECT_ID = 'oceanic-granite-413404'
    DATASET_ID = 'HighD_meta'
    OUTPUT_TABLE_ID = 'oceanic-granite-413404:HighD_meta_filtered.data2'
    pipeline_args = [
        '--project=' + PROJECT_ID,
        '--temp_location=gs://oceanic-granite-413404-bucket/temp/bq_highD',
    ]

    run_union_pipeline(PROJECT_ID, DATASET_ID, OUTPUT_TABLE_ID, pipeline_args)

