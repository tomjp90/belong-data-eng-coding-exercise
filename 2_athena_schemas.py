#import AWS keys from secrets.py
from secrets import access_key, secret_access_key
import boto3
import time 
import os
import csv


print('Executing schemas query...')
# create a client to connect to AWS server
client = boto3.client('athena',
                    region_name = 'ap-southeast-2',
                           aws_access_key_id = access_key,
                            aws_secret_access_key = secret_access_key
                        )

queryStart = client.start_query_execution(
                                          QueryString = f'''CREATE EXTERNAL TABLE IF NOT EXISTS default.sensor_id (
                                                            `sensor_id` int,
                                                            `sensor_description` string,
                                                            `sensor_name` string,
                                                            `installation_date` timestamp,
                                                            `status` string,
                                                            `note` string,
                                                            `direction_1` string,
                                                            `direction_2` string,
                                                            `latitude` float,
                                                            `longitude` float,
                                                            `location` string 
                                                            )
                                                            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                                                            WITH SERDEPROPERTIES (
                                                            'serialization.format' = ',',
                                                            'field.delim' = ','
                                                            ) LOCATION 's3://ped-counts-2009-present-melb/ped_ids/data/'
                                                            TBLPROPERTIES ('has_encrypted_data'='true');''',
                        
                                          QueryExecutionContext = {
                                                'Database': 'pedestrian_counts'
                                          },
                                          ResultConfiguration = {
                                          'OutputLocation': 's3://ped-counts-2009-present-melb/Queries' 
                                          }
                                          )
    
#execute query
queryId = queryStart['QueryExecutionId']
time.sleep(10)
results = client.get_query_results(QueryExecutionId = queryId)

queryStart = client.start_query_execution(
                                          QueryString = f'''CREATE EXTERNAL TABLE IF NOT EXISTS default.ped_counts (
                                                            `id` int,
                                                            `date_time` timestamp,
                                                            `year` int,
                                                            `month` string,
                                                            `mdate` int,
                                                            `day` string,
                                                            `time` int,
                                                            `sensor_id` int,
                                                            `sensor_name` string,
                                                            `hourly_counts` int 
                                                            )
                                                            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                                                            WITH SERDEPROPERTIES (
                                                            'serialization.format' = ',',
                                                            'field.delim' = ','
                                                            ) LOCATION 's3://ped-counts-2009-present-melb/ped_counts/data/'
                                                            TBLPROPERTIES ('has_encrypted_data'='false');''',
                        
                                          QueryExecutionContext = {
                                                'Database': 'pedestrian_counts'
                                          },
                                          ResultConfiguration = {
                                          'OutputLocation': 's3://ped-counts-2009-present-melb/Queries' 
                                          }
                                          )
    
#execute query
queryId = queryStart['QueryExecutionId']
time.sleep(10)
results = client.get_query_results(QueryExecutionId = queryId)

print('Tables successfully created in Athena!')
print('### Complete! ###')


