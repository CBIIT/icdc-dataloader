# This file is used as a AWS Lambda function to unzip/untar raw files, and send messages to file loader
import boto3
import uuid
import re
from urllib.parse import unquote_plus
import logging
import zipfile
import tarfile
import glob
import os
from utils import *

RAW_PREFIX = 'RAW'
FINAL_PREFIX = 'Final'

log = get_logger('Raw File Processor')
s3_client = boto3.client('s3')


def download_extract_file(bucket, key, download_path, extract_path):
    """
        Download and extract files
    """
    ext = key.split('.')[-1]
    if ext:
        if ext == 'zip':
            log.info('downloading zip file: {}'.format(key))
            s3_client.download_file(bucket, key, download_path)
            with zipfile.ZipFile(download_path, "r") as zip_ref:
                log.info('Extracting zip file: {}'.format(key))
                zip_ref.extractall(extract_path)
        elif ext == 'tar':
            log.info('downloading tar file: {}'.format(key))
            s3_client.download_file(bucket, key, download_path)
            with tarfile.TarFile(download_path, "r") as tar_ref:
                log.info('Extracting tar file: {}'.format(key))
                tar_ref.extractall(extract_path)
        elif ext == 'gz':
            log.warning('{} file is not supported!'.format(ext))
            return -1
        else:
            log.warning('{} file is not supported!'.format(ext))
            return -1

def upload_files(bucket, upload_path, src_folder):
    file_list = glob.glob('{}/*'.format(src_folder))
    for file in file_list:
        if os.path.isfile(file):
            log.info('Uploading file: {}'.format(file))
            file_name = os.path.join(upload_path, os.path.basename(file))
            s3_client.upload_file(file, '{}resized'.format(bucket), file_name)
        else:
            log.info('{} is not a file, and won\'t be uploaded to S3'.format(file))


def handler(event, context):
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            # Assume raw files will be in a sub-folder inside '/RAW'
            if not key.startswith(RAW_PREFIX):
                log.error('File is not in {} folder'.format(RAW_PREFIX))
                return -1
            org_path = os.path.dirname(key).replace(RAW_PREFIX, FINAL_PREFIX)
            org_file_name = os.path.basename(key)
            download_path = '/tmp/{}{}'.format(uuid.uuid4(), org_file_name)
            extracted_path = '/tmp/{}'.format(uuid.uuid4())
            download_extract_file(bucket, key, download_path, extracted_path)
            upload_files(bucket, org_path, extracted_path)


    except Exception as e:
        log.exception(e)
