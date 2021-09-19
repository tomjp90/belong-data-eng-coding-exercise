# import AWS keys from secrets.py
from secrets import access_key, secret_access_key
import boto3
import time
import os
import csv

# create a client to connect to AWS server
client = boto3.client('athena',
                    region_name='ap-southeast-2',
                           aws_access_key_id=access_key,
                            aws_secret_access_key=secret_access_key
                        )


print("-- Querying days.... 1.Top 10 (most pedestrians) locations by day ")
data = []
# loop the days and query them
days = ['Monday', 'Tuesday', 'Wednesday',
    'Thursday', 'Friday', 'Saturday', 'Sunday']

for day in days:
    print(f"Querying {day}..")

    # creater a client and query
    queryStart = client.start_query_execution(
                              QueryString=f'''SELECT pc.day, pc.sensor_id, SUM(pc.hourly_counts) as Total_Pedestrians, si.sensor_description, si.sensor_name, si.latitude, si.longitude

                                                FROM ped_counts as pc

                                                INNER JOIN sensor_id as si
                                                ON (pc.sensor_id = si.sensor_id)

                                                WHERE pc.day = '{day}'

                                                GROUP BY pc.sensor_id, pc.day, si.sensor_description, si.sensor_name, si.location, si.latitude, si.longitude

                                                ORDER BY SUM(pc.hourly_counts) DESC

                                                LIMIT 10 ''',

                              QueryExecutionContext={
                                    'Database': 'default'
                              },
                              ResultConfiguration={
                              'OutputLocation': 's3://ped-counts-2009-present-melb/Queries'
                              }
                                                      )

    # execute query
    queryId = queryStart['QueryExecutionId']
    time.sleep(10)
    results = client.get_query_results(QueryExecutionId=queryId)

    print("Storing query reults")

    # loop and append results from query and create list
    for row in results['ResultSet']['Rows'][1:]:
        csv_list = []
        for value in row['Data']:
            csv_list.append(value['VarCharValue'])
         
            if len(csv_list) == 7:
                data.append(csv_list)

# clear any csv if it exists
if os.path.exists('results/top_by_day.csv'):
    os.remove('results/top_by_day.csv')

print("Writing top_by_day.csv")
# write data to csv in results
with open('results/top_by_day.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)
    # write multiple rows
    writer.writerows(data)


print("-- Querying months.... 2.Top 10 (most pedestrians) locations by month ")
data = []

# loop the days and query them
months = ['January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December']
for month in months:
    print(f"Querying {month}..")

    # creater a query for client
    queryStart = client.start_query_execution(
                              QueryString=f'''SELECT pc.month, pc.sensor_id, SUM(pc.hourly_counts) as Total_Pedestrians, si.sensor_description, si.sensor_name, si.latitude, si.longitude

                                                FROM ped_counts as pc

                                                INNER JOIN sensor_id as si
                                                ON (pc.sensor_id = si.sensor_id)

                                                WHERE pc.month = '{month}'

                                                GROUP BY pc.sensor_id, pc.month, si.sensor_description, si.sensor_name, si.location, si.latitude, si.longitude

                                                ORDER BY SUM(pc.hourly_counts) DESC

                                                LIMIT 10 ''',

                              QueryExecutionContext={
                                    'Database': 'default'
                              },
                              ResultConfiguration={
                              'OutputLocation': 's3://ped-counts-2009-present-melb/Queries'
                              }
                                                      )

    # execute query
    queryId = queryStart['QueryExecutionId']
    time.sleep(10)
    results = client.get_query_results(QueryExecutionId=queryId)

    print("Storing query reults")
    # print(results)

    # loop and append results from query and create list
    for row in results['ResultSet']['Rows'][1:]:
        csv_list = []
        for value in row['Data']:
            csv_list.append(value['VarCharValue'])
            if len(csv_list) == 7:
                  
                  data.append(csv_list)

# clear any csv if it exists
if os.path.exists('results/top_by_month.csv'):
    os.remove('results/top_by_month.csv')
                         
print("Writing top_by_month.csv")               
# write data to csv in results
with open('results/top_by_month.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)
    # write multiple rows
  
    writer.writerows(data)


print("CSVs with query results saved in /results")
print('### Complete! ####')
