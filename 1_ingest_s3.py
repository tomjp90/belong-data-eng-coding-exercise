# import keys from secrets.py
from secrets import access_key, secret_access_key
# import AWS SDK for Python
import boto3

import os
import requests


try:
      print("--- Downloading latest files.. ---")
      print("This may take a several minutes depending on your internet speed")

      # -------- Pedestrian counts for Melb from 2009
      file_url = 'https://data.melbourne.vic.gov.au/api/views/b2ak-trbp/rows.csv?accessType=DOWNLOAD'
      data = requests.get(file_url)
      open('data/ped_counts.csv', 'wb').write(data.content)
      print("Successfully downloaded and updated data/ped_counts.csv")

      # --------- Sensor information
      id_url = 'https://data.melbourne.vic.gov.au/api/views/h57g-5234/rows.csv?accessType=DOWNLOAD'
      ped_id = requests.get(id_url)
      open('data/ped_ids.csv', 'wb').write(ped_id.content)
      print("Successfully downloaded and updated data/ped_ids.csv")

      ped_counts = 'data/ped_counts.csv'
      ped_ids = 'data/ped_ids.csv'
      print("Successfully downloaded all updated CSVs")

      print("--- Uploading/updating latest CSVs to s3 ")
      print("This may take a several minutes depending on your internet speed")
      # create a client to s3
      client = boto3.client('s3',
                              aws_access_key_id = access_key,
                              aws_secret_access_key = secret_access_key
                              )
  
      upload_file_bucket = 'ped-counts-2009-present-melb'
      
      upload_file_key = 'ped_counts/' + str(ped_counts)
      print(f"Uploading {upload_file_key}...")
      client.upload_file(ped_counts, upload_file_bucket, upload_file_key)
      print(f"Successfully uploaded {ped_counts} to s3 in {upload_file_bucket} bucket")

      upload_file_key = 'ped_ids/' + str(ped_ids)
      print(f"Uploading {upload_file_key}...")
      client.upload_file(ped_ids, upload_file_bucket, upload_file_key)
      print(f"Successfully uploaded {ped_ids} to s3 in {upload_file_bucket} bucket")
except:
      print("Error")


print("### Complete! ###")
