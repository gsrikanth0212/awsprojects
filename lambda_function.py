import json
import pandas as pd
import boto3
from io import StringIO


def read_s3_data_and_process(s3client, bucket, key):
    try:
        response = s3client.get_object(Bucket = bucket, Key = key)
        df = pd.read_csv(response['Body'], sep = '\t', dtype = 'str')
        print(df.head(5))
        transformed_df = df[['Order ID', 'Product Name', 'Sales']]
        print(transformed_df.head(5))
        
        return transformed_df
    
    except Exception as e:
        print(e) 
        
def list_files(s3client, bucket, prefix = ''):
    files = []
    try:
        response = s3client.list_objects_v2(Bucket = bucket, Prefix = prefix)
        if 'Contents' in response:
            for item in response['Contents']:
                if not item['Key'].endswith('/'):
                    files.append(item['Key'])
        return files
    except Exception as e:
        print(e)

def write_transformed_data(df, bucket, key):
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
    
    
def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket = 'ncodedata-bucket'
    
    file_list = list_files(s3_client, bucket, 'datafiles')
    print('file_list: ',file_list)
    
    for key in file_list:
        file_name = key.split('/')[-1]
        output_folder = 'output'
        output_file_name = 'processed_'+ file_name
        output_key = f'{output_folder}/{output_file_name}'
        
        transformed_df = read_s3_data_and_process(s3_client, bucket, key)
        write_transformed_data(transformed_df, bucket, output_key)
    
    print('Files processed successfully')
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
