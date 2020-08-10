# AWS-S3-Lambda_Function

s3_operarion.py script lets you create a new bucket, upload file into a bucket, reverse the file content using lambda function, list all bucket, empty, and delete the bucket.

## Here I have used Python v3.7 and Boto3 which is a AWS SDK for Python.

## Prerequisites for Installation :

Create lambda function called 's3-trigger' and paste 'lambda_function.py' code into it.


All the required steps for installation are executed using below command.

        pip install -r requirements.txt


## Steps to execute the program:

Below are the commands mentioned to do various tasks as per requirements.

### Case 1: To Create new S3 Bucket:

        python s3_operations.py create --name sanket123

### Case2: To Upload file into bucket:

First add this bucket's trigger into s3-trigger lambda function. Select 'PUT' as event type while doing it.It checks random name file every second and return the reverse string.

        python s3_operations.py upload --name sanket123 --text hello

### Case3: To List all available bucket:

        python s3_operations.py list

### Case4: To Empty bucket before deletion: ( Required Before deleting any bucket)

        python s3_operations.py empty --name sanket123

### Case5: To Delete bucket:

        python s3_operations.py delete --name sanket123


### To Test previous script s3_operations.py:

        python test.py

### Issues

        If any error has occurred, please let me know by opening issue.
