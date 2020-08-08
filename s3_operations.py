import argparse
import boto3
from boto3 import Session
import uuid
import threading
from botocore.exceptions import ClientError


class S3Operations:

    session = Session(profile_name='default')
    s3 = boto3.client('s3')
    file_name = None
    bucket_name = None

    @staticmethod
    def empty_bucket(name):
        """Empty bucket before delete"""
        s3_resource = boto3.resource('s3')
        s3_bucket = s3_resource.Bucket(name)
        objects_to_delete = []
        for obj in s3_resource.Bucket(name).objects.all():
            objects_to_delete.append({'Key': obj.key})
        if objects_to_delete:
            s3_bucket.delete_objects(
                Delete={
                    'Objects': objects_to_delete
                }
            )

    @staticmethod
    def get_buckets():
        """List of bucket"""
        s3_resource = boto3.resource('s3')
        print("List of buckets:\n")
        for bucket in s3_resource.buckets.all():
            print(bucket.name)

    @staticmethod
    def delete_bucket(name):
        """Delete bucket"""
        s3_resource = boto3.resource('s3')
        s3_bucket = s3_resource.Bucket(name)
        try:
            s3_resource.Bucket(name).delete()
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketNotEmpty':
                return f"{s3_bucket} " \
                    f"The bucket you tried to delete is not empty"
            else:
                raise e
        return

    def create_bucket(self, name):
        """Create new bucket"""
        s3_resource = boto3.resource('s3')
        try:
            s3_resource.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.session.region_name
                }
            )
        except ClientError as e:
            s3_bucket = s3_resource.Bucket(name)
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                return f"{s3_bucket} " \
                    f"This bucket is already owned by you"
            elif e.response['Error']['Code'] == 'BucketAlreadyExists':
                return f"{s3_bucket} " \
                    f"Please select a different name and try again."
            else:
                raise e
        return

    def upload_file(self, name, text):
        """Upload file into bucket"""
        file_name = str(uuid.uuid4().hex[:6])
        self.s3.put_object(
            Bucket=name,
            Body=text,
            Key=file_name
        )
        self.file_name = file_name
        self.bucket_name = name
        return file_name

    def read_file(self, name):
        """
        :param name: File name
        :return: Bool
        """
        value = False
        try:
            data = self.s3.get_object(Bucket="revfilecontent", Key=name)
            contents = data['Body'].read().decode('utf-8')
            print(contents)
            value = True
        except Exception as e:
            print(e)
        return value

    def set_interval(self, time, name):
        """check file every second"""
        e = threading.Event()
        while not e.wait(time):
            print("checking")
            r = self.read_file(name)
            if r:
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", help="Select operation", choices=["create", "upload", "list", "empty", "delete"])
    parser.add_argument("--name", help="Bucket name")
    parser.add_argument("--text", help="Data to insert into file")

    args = parser.parse_args()

    print('Bucket name: ', args.name)
    print('Data:', args.text)

    driver_s3 = S3Operations()
    if args.operation == "create":
        driver_s3.create_bucket(args.name)

    elif args.operation == "upload":
        key = driver_s3.upload_file(args.name, args.text)
        print("file_name: " + key)
        driver_s3.set_interval(1, key)

    elif args.operation == "empty":
        S3Operations.empty_bucket(args.name)

    elif args.operation == "delete":
        S3Operations.empty_bucket(args.name)
        S3Operations.delete_bucket(args.name)

    elif args.operation == "list":
        driver_s3.get_buckets()

    else:
        print("unsupported operation")
