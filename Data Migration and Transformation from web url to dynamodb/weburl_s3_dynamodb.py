# importing necessary modules
import io
import requests
import zipfile
import boto3
import json

# Creating an Amazon s3 bucket

s3_client = boto3.client(
            's3',
            aws_access_key_id='AKIAQPDEPZYISLTYVGGA',
            aws_secret_access_key='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',
            region_name='us-east-1'
        )
s3_client.create_bucket(Bucket='doi077')
print('Bucket created')

# Retrieve the list of existing buckets
response = s3_client.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

print('Downloading started from Web url')

# Defining the zip file URL
url = 'http://localhost:8080/etl/download/json'

# Downloading the file by sending the request to the URL
req = requests.get(url)
print('Downloading Completed')

data = req.content
print(data)

# Connecting to s3 using Boto3
s3_client = boto3.client(
            's3',
            aws_access_key_id='AKIAQPDEPZYISLTYVGGA',
            aws_secret_access_key='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',
            region_name='us-east-1'
        )

# Uploading the zip file contents from Web url
s3_client.upload_fileobj(io.BytesIO(data), "doi077", "test.zip")

# To get the zip file from s3 and extract:
# Getting the zip file object from s3.
z = s3_client.get_object(Bucket="doi077", Key="test.zip")
print(type(z))

# Reading the zip file object from s3
zi = io.BytesIO(z['Body'].read())
print(zi)

# Putting back the file object to s3 and reading it in s3.
list_files = []
folder_name = 'unzip'
with zipfile.ZipFile(zi, mode='r') as zipf:
    print(zipf.infolist())
    for file in zipf.infolist():
        print(file)
        file_name = file.filename
        putFile = s3_client.put_object(Body=zipf.read(file), Bucket="doi077", Key=f"{folder_name}/{file_name}")
        list_files.append(putFile)
print(list_files)
print("Successfully Unzipped in s3")

s3_object = s3_client.list_objects(Bucket="doi077", Prefix=folder_name)['Contents']
print(s3_object)

list_filename = []
for filename in s3_object:
    list_filename.append(filename.get('Key').split('/')[-1])
print(list_filename)

s3_client = boto3.resource(
    's3',
    aws_access_key_id='AKIAQPDEPZYISLTYVGGA',
    aws_secret_access_key='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',
    region_name='us-east-1'
)
bucket_name = 'doi077'
bucket = s3_client.Bucket("doi077")

file_names = []
for obj in bucket.objects.all():
    file_names.append(obj.key)
print(file_names)


def convert_to_string(data):
    if isinstance(data, float):
        return str(data)
    elif isinstance(data, dict):
        return {k: convert_to_string(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_to_string(elem) for elem in data]
    else:
        return data


# Create a boto3 client to access the DynamoDB service
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id='AKIAQPDEPZYISLTYVGGA',
    aws_secret_access_key='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',
    region_name='us-east-1'
)

print(dynamodb)

#
table = dynamodb.Table('json_zip')
print("Table object: {}".format(table))

prefix = f'{folder_name}/'


for file in list_filename:
    print(f"printing filename: {file}")
    s3_object = s3_client.Object(bucket_name, prefix + file)
    print(s3_object)
    s3_data = s3_object.get()['Body'].read().decode('utf-8')
    json_data = json.loads(s3_data)

    data = json.dumps(json_data, separators=(',', ':'), default=str)
    json_dict = json.loads(data)
    string_data = convert_to_string(json_dict)
    if string_data.get("cik"):
        table.put_item(
            Item=string_data
        )
        print(f'success - {file}')
    else:
        pass


print("Data moved into DynamoDB successfully")




