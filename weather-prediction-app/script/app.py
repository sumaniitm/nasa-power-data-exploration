import json
import boto3
import fsspec
import numpy as np
import xarray as xr
import os
import numpy as np
import pandas as pd
from datetime import datetime

sts = boto3.client('sts')

assumed_role = sts.assume_role(
    RoleArn='arn:aws:iam::690817015997:role/nasa-power-s3-access-role',
    RoleSessionName='NasaPowerDataExplorationSession2'  # Provide a session name
)

# Extract temporary credentials from the assumed role response
credentials = assumed_role['Credentials']

aws_access_key_id = credentials['AccessKeyId']
aws_secret_access_key = credentials['SecretAccessKey']
aws_session_token = credentials['SessionToken']

s3_client = boto3.client('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key
                         , aws_session_token = aws_session_token)

filepath = 'https://power-analysis-ready-datastore.s3.amazonaws.com/power_901_daily_meteorology_utc.zarr'
filepath_mapped = fsspec.get_mapper(filepath)

ds_daily = xr.open_zarr(store=filepath_mapped, consolidated=True)

kol_daily_dask_df = ds_daily.sel(lat=22, lon=87.5).to_dask_dataframe()
kol_daily_dask_df = kol_daily_dask_df.dropna()
local_path = os.path.join('kolkata_901_daily_meteorology_utc_dask')
kol_daily_dask_df.to_parquet(local_path)

for file in os.listdir(local_path):
    print(file)
    local_path_to_file = "/".join([local_path, file])
    print(local_path_to_file)
    s3_client.upload_file(local_path_to_file, 'nasa-power-combined-data', file)

bucket_contents = s3_client.list_objects_v2(Bucket='nasa-power-combined-data')
list_of_objs_in_bucket = bucket_contents['Contents']
download_path_local_root = 's3_downloads'
for i in range(len(list_of_objs_in_bucket)):
    print(list_of_objs_in_bucket[i]['Key'])
    download_path_local = "/".join([download_path_local_root, list_of_objs_in_bucket[i]['Key']])
    print(download_path_local)
    s3_client.download_file('nasa-power-combined-data', list_of_objs_in_bucket[i]['Key'], download_path_local)

df = pd.read_parquet(download_path_local_root)
print(df.shape)
