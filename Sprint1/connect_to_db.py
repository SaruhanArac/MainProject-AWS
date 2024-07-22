# import psycopg2
# import boto3
# import json
# import os

# ssm_client = boto3.client('ssm')

# def get_ssm_param(param_name):
#     """Get the SSM Param from AWS and turn it into JSON."""
#     print(f'get_ssm_param: getting param_name={param_name}')
#     parameter_details = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
#     redshift_details = json.loads(parameter_details['Parameter']['Value'])

#     host = redshift_details['host']
#     user = redshift_details['user']
#     db = redshift_details['database-name']
#     print(f'get_ssm_param loaded for db={db}, user={user}, host={host}')
#     return redshift_details

# def open_sql_database_connection_and_cursor(redshift_details):
#     """Use the redshift details JSON to connect to the database."""
#     print('open_sql_database_connection_and_cursor: new connection starting...')
#     db_connection = psy.connect(
#         host=redshift_details['host'],
#         database=redshift_details['database-name'],
#         user=redshift_details['user'],
#         password=redshift_details['password'],
#         port=redshift_details['port']
#     )
#     cursor = db_connection.cursor()
#     print('open_sql_database_connection_and_cursor: connection ready')
#     return db_connection, cursor
