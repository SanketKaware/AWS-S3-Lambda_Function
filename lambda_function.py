import json
import boto3
import uuid
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def reverse(s):
    s = list(s)

    i = 0
    j = len(s) - 1

    while i < j:
        s[i], s[j] = s[j], s[i]
        j -= 1
        i += 1
    return "".join(s)


def lambda_handler(event, context):
    for record in event['Records']:
        print("record")
        print(record)
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        print(bucket)
        print(key)

        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        upload_path = '/tmp/resized-{}'.format(key)
        s3_client.download_file(bucket, key, download_path)

        content = None
        with open(download_path, "r") as file:
            content = file.read()
            print(content)
            content = reverse(content)
            print(content)

        # with open(download_path,"a+") as file:
        # file.write(content)

        # file=open(upload_path,"w")
        # file.write(content)
        # file.close()

        up_bucket = "revfilecontent"
        # s3_bucket = s3_resource.Bucket(bucket)
        # s3_bucket.delete_objects(
        #         Delete={
        #             'Objects': [{'Key':key}]
        #         }
        #     )

        s3_client.put_object(Bucket=up_bucket, Body=content, Key=key)

        # s3_client.upload_file(upload_path,bucket,key)





