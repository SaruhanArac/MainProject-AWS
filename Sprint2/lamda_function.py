import json
import boto3
import os
import psycopg2 as psy
from extract_function import *

from schema import *

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')

def get_ssm_param(param_name):
    """Get the SSM Param from AWS and turn it into JSON."""
    print(f'get_ssm_param: getting param_name={param_name}')
    parameter_details = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
    redshift_details = json.loads(parameter_details['Parameter']['Value'])

    host = redshift_details['host']
    user = redshift_details['user']
    db = redshift_details['database-name']
    print(f'get_ssm_param loaded for db={db}, user={user}, host={host}')
    return redshift_details

def open_sql_database_connection_and_cursor(redshift_details):
    """Use the redshift details JSON to connect to the database."""
    print('open_sql_database_connection_and_cursor: new connection starting...')
    db_connection = psy.connect(
        host=redshift_details['host'],
        database=redshift_details['database-name'],
        user=redshift_details['user'],
        password=redshift_details['password'],
        port=redshift_details['port']
    )
    cursor = db_connection.cursor()
    print('open_sql_database_connection_and_cursor: connection ready')
    return db_connection, cursor

def lambda_handler(event, context):
    bucket = "cremedelacreme-bucket"
    filename = "chesterfield_25-08-2021_09-00-00.csv"
    download_path = '/tmp/' + filename

    ssm_param_name = os.environ.get('SSM_PARAMETER_NAME', 'NOT_SET')
    print(f'lambda_handler: ssm_param_name={ssm_param_name}')

    redshift_details = get_ssm_param(ssm_param_name)
    conn, cur = open_sql_database_connection_and_cursor(redshift_details)
    create_tables(conn)
    
    extract_csv_from_s3(bucket, filename, download_path)
    data = extract_csv_to_list(download_path)
    transformed_data = transform_data(data)
    
    insert_location_data(conn, transformed_data)
    insert_payment_method_data(conn, transformed_data)
    insert_transaction_data(conn, transformed_data)
    insert_item_data(conn, transformed_data)

    conn.commit()
    cur.close()
    conn.close()

    print(f'lambda_handler: done')
