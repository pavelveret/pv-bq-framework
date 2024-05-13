
import pandas as pd
pd.set_option('display.max_columns', None)
from google.cloud import bigquery
import numpy as np
import environ

env = environ.Env()
environ.Env.read_env()

SA_PATH = env('GOOGLE_SERVICE_ACCOUNT_PATH')
env_project = env('BQ_PROJECT')

# %%
def bq_client(bq_project = env_project):
    CREDS = SA_PATH
    client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS, project=bq_project)
    return client

# %%
def bq_hint():
    
    hint_1 = """1. generate_bq_schema(dictionary). Takes dictionary of Pandas columns and types and returnes BQ schema"""
    hint_2 = """2. create_bigquery_table(dataset_name, table_name, schema) creates table in bq"""
    
    hints_list = [hint_1, hint_2]
    line = ""
    for hint in hints_list:
        line = line + hint + "\n"
    
    print(line)

# %%
def generate_bq_schema(dictionary):
    schema_list = []
    for col_name, col_type in dictionary.items():
        if col_type in ['int', 'int8', 'int16', 'int32', 'int64']:
            bq_type = 'INTEGER'
        elif col_type in ['float', 'float16', 'float32', 'float64']:
            bq_type = 'FLOAT'
        elif col_type == 'bool':
            bq_type = 'BOOLEAN'
        elif col_type in ['str', 'string']:
            bq_type = 'STRING'
        elif "datetime64" in col_type:
            bq_type = 'DATETIME'
        else:
            raise ValueError('Type is not defined')
        
        field = bigquery.SchemaField(col_name, bq_type)
        schema_list.append(field)
    
    return schema_list
        

# %%
def create_bigquery_table(dataset_name, table_name, schema, partition_field=None, bq_project = env_project):
    # Construct a BigQuery client object.
    client = bq_client()

    # Set table_id to the ID of the table to create.
    table_id = f"{bq_project}.{dataset_name}.{table_name}"

    # Use the SchemaField class to create the schema
    table_schema = schema
    # Create a Table object
    table = bigquery.Table(table_id, schema=table_schema)
    
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,  # name of column to use for partitioning
            expiration_ms=1000 * 60 * 60 * 24 * 90,
        )  # 90 days

    # Make an API request to create the table
    table = client.create_table(table)  # Make an API request.
    print(f"Created table {bq_project}.{table.dataset_id}.{table.table_id}")

# %%
def load_df_to_bq(dataset_name, table_name, schema, df, partition_field=None, bq_project = env_project):
    client = bq_client()
    table_id = f"{bq_project}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    if partition_field: 
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )   

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

# %%
def append_df_to_bq(dataset_name, table_name, schema, df, partition_field=None, bq_project = env_project):
    client = bq_client()
    table_id = f"{bq_project}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    if partition_field: 
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )   

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    return job.result()
# %%

def fetch_data_from_bigquery(sql_query, bq_project = env_project):
    """
    Fetches data from Google BigQuery using a provided SQL query and service account credentials.

    Args:
    sql_query (str): SQL query to execute.
    json_credentials_path (str): Path to the JSON credentials file for the service account.

    Returns:
    pandas.DataFrame: The result of the query as a DataFrame.
    """
    # Set the credentials using the service account JSON file
    client = bq_client()
    # Create a job to execute the query
    query_job = client.query(sql_query)

    # Return the results as a Pandas DataFrame
    return query_job.to_dataframe()

def materialize_view(dataset, source_view, output_name=None, bq_project = env_project):
    """
    Materializes a BigQuery view into a table with a prefix 'MT_'.

    Args:
    source_view (str): The name of the source view.
    """
    client = bq_client()
    if not output_name:
        destination_table = f"MT_{source_view}"
    else:
        destination_table = output_name
    
    # Construct SQL to check if the destination table exists and delete it if it does
    check_query = f"""
    SELECT table_name
    FROM `{bq_project}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = '{destination_table}'
    """

    job = client.query(check_query)
    results = list(job.result())

    if results:
        # Table exists, so delete it
        delete_query = f"DROP TABLE `{bq_project}.{dataset}.{destination_table}`"
        client.query(delete_query).result()

    # Create the destination table by selecting all from the view
    create_table_query = f"""
    CREATE TABLE `{bq_project}.{dataset}.{destination_table}`
    AS SELECT * FROM `{bq_project}.{dataset}.{source_view}`
    """
    
    client.query(create_table_query).result()

    print(f"View {source_view} has been materialized into table {destination_table} in project {bq_project}")
# %%
