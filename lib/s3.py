#!/usr/bin/env python
import boto3
import botocore
from .utils import get_logger
import os

class S3Bucket:
    def __init__(self, bucket):
        self.bucket_name = bucket
        self.client = boto3.client('s3')
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        self.log = get_logger('S3 Bucket')

    def upload_file_obj(self, key, data):
        return self.bucket.put_object(Key=key, Body=data)

    def download_file(self, key, filename):
        return self.bucket.download_file(key, filename)

    def download_file_obj(self, key, obj):
        self.bucket.download_fileobj(key, obj)

    def delete_file(self, key):
        response = self.bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key
                    }
                ]
            }
        )
        if 'Errors' in response:
            self.log.error('S3: delete file {} failed!'.format(key))
            return False
        else:
            return True

    def upload_file(self, key, fileName):
        with open(fileName, 'rb') as data:
            obj = self.upload_file_obj(key, data)
            if obj:
                return {'bucket': self.bucket.name, 'key': key}
            else:
                message = "Upload file {} to S3 failed!".format(fileName)
                self.log.error(message)
                return None

    def download_files_in_folder(self, folder, local_path):
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            result = self.client.list_objects_v2(Bucket=self.bucket_name,  Prefix=folder)
            for file in result.get('Contents', []):
                if file['Size'] > 0:
                    key = file['Key']
                    base_name = os.path.basename(key)
                    file_name = os.path.join(local_path, base_name)
                    self.log.info('Downloading "{}" from AWS S3'.format(base_name))
                    self.download_file(key, file_name)
            return True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                self.log.error('Don\'t have permission to access for Bucket: "{}"'.format(self.bucket_name))
            elif error_code == 404:
                self.log.error('Bucket: "{}" does NOT exist!'.format(self.bucket_name))
            return False
