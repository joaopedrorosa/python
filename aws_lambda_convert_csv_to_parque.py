import pandas as pd
import boto3

from s3fs         import S3FileSystem
from urllib.parse import unquote

# Function used to create S3 client for file retrieval and an S3 Filesystem client
def initClients():
    s3 = boto3.client('s3')
    s3_fs  = S3FileSystem()
    return s3, s3_fs

# Function used to get metadata from the file that triggers the Lambda function
def getEventMetadata(event):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = unquote(event['Records'][0]['s3']['object']['key'])
    return bucket_name, file_key

# Function used to retrieve objects from S3
def retrieveS3Object(client_s3, bucket, file_key):
    file_response = client_s3.get_object(Bucket=bucket, Key=file_key)
    file_data = file_response['Body']
    return file_data

# Function used to write a Parquet dataset to S3
def writeToParquet(dataframe, file_system, parquet_output_path, partition_columns):
    dataframe.to_parquet(path = parquet_output_path,
                         filesystem = file_system,
                         engine = 'pyarrow',
                         partition_cols = partition_columns,
                         allow_truncated_timestamps=True)

# Clients initialization
s3, s3_fs = initClients()

# Specification of S3 folder and File column used to partition the data
s3_target = 'my-test-bucket-jrosa/tesla-historical-stock-price-parquet'
partition_column = 'Date_Month'

# Main Handler
def lambda_handler(event, context):
    # Retrieve metadata from event-file
    bucket_name, file_key = getEventMetadata(event)
    # Retrieve S3 object and open gzip file to retrieve lines
    file_data = retrieveS3Object(s3, bucket_name, file_key)
    # Create Dataframe from S3 object
    df = pd.read_csv(file_data, sep=',')
    # Writes dataframe to S3 in Parquet format
    writeToParquet(df, s3_fs, s3_target, partition_column)
