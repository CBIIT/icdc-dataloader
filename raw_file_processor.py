# This file is used as a AWS Lambda function to unzip/untar raw files, and send messages to file loader
# It needs to read SQS queue name from environment variable QUEUE_NAME

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
from sqs import Queue
import json
import csv
import hashlib

RAW_PREFIX = 'RAW'
FINAL_PREFIX = 'Final'
log = get_logger('Raw File Processor')
s3_client = boto3.client('s3')
queue = Queue(QUEUE_NAME)
ICDC_FILE_UPLOADED = 'ICDC-file-uploaded'
FILE_NAME = 'file_name'
CASE_ID = 'case_id'
MD5 = 'md5sum'
FILE_SIZE = "file_size"
FILE_LOC = 'file_location'
FILE_FORMAT = 'file_format'
UUID = 'uuid'
FILE_STAT = 'file_status'
ACL = 'acl'
MANIFEST_FIELDS = [UUID, FILE_SIZE, MD5, FILE_STAT, FILE_LOC, FILE_FORMAT, ACL]
BLOCK_SIZE = 65536
DEFAULT_STAT = 'uploaded'
DEFAULT_ACL = 'open'
MANIFESTS = 'manifests'
FILES = 'files'

# Download and extract files
def download_extract_file(bucket, key, download_path, extract_path) -> bool:
    try:
        ext = key.split('.')[-1]
        if ext:
            if ext == 'zip':
                log.info('downloading zip file: {}'.format(key))
                s3_client.download_file(bucket, key, download_path)
                with zipfile.ZipFile(download_path, "r") as zip_ref:
                    log.info('Extracting zip file: {}'.format(key))
                    zip_ref.extractall(extract_path)
                return True
            elif ext == 'tar':
                log.info('downloading tar file: {}'.format(key))
                s3_client.download_file(bucket, key, download_path)
                with tarfile.TarFile(download_path, "r") as tar_ref:
                    log.info('Extracting tar file: {}'.format(key))
                    tar_ref.extractall(extract_path)
                return True
            elif ext == 'gz':
                log.warning('{} file is not supported!'.format(ext))
                return False
            else:
                log.warning('{} file is not supported!'.format(ext))
                return False
        else:
            return False
    except Exception as e:
        log.exception(e)
        return False

def upload_files(bucket, upload_path, file_list):
    try:
        for file in file_list:
            if os.path.isfile(file):
                log.info('Uploading file: {}'.format(file))
                file_name = os.path.join(upload_path, os.path.basename(file))
                s3_client.upload_file(file, bucket, file_name)
            else:
                log.info('{} is not a file, and won\'t be uploaded to S3'.format(file))
        return True
    except Exception as e:
        log.exception(e)
        return False


def send_sqs_message(queue, data_bucket, data_path):
    try:
        obj = {
            'type': ICDC_FILE_UPLOADED,
            'bucket': data_bucket,
            'path': data_path
        }
        queue.sendMsgToQueue(json.dumps(obj), data_path)
        log.info('Data path: {}'.format(data_path))
        return True
    except Exception as e:
        log.exception(e)
        return False

def get_s3_location(bucket, folder, key):
    return "s3://{}/{}/{}".format(bucket, folder, key)

def populate_record(record, data_folder, bucket, s3_folder):
    file_name = record[FILE_NAME]
    data_file = os.path.join(data_folder, file_name)
    record[FILE_SIZE] = os.stat(data_file).st_size
    record[FILE_LOC] = get_s3_location(bucket, s3_folder, file_name)
    record[FILE_FORMAT] = (os.path.splitext(data_file)[1]).split('.')[1].lower()
    record[UUID] = get_uuid_for_node("file", record[FILE_LOC])
    hasher = hashlib.md5()
    with open(data_file, 'rb') as afile:
        buf = afile.read(BLOCK_SIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCK_SIZE)
    record[MD5] = hasher.hexdigest()
    record[FILE_STAT] = DEFAULT_STAT
    record[ACL] = DEFAULT_ACL

    return record

# check the field file_name/case id in the manifest which should not be null/empty
# check files included in the manifest exist or not
def populate_manifest(manifest, data_folder, bucket, s3_folder):
    log.info('Validating manifest: {}'.format(manifest))
    succeeded = True
    final_files = []
    # check manifest
    if not os.path.isfile(manifest):
        log.error('Manifest: "{}" does not exists !'.format(manifest))
        succeeded = False
    else:
        try:
            log.info('Processing fields in manifest.')
            # check fields in the manifest, if missing fields stops
            with open(manifest) as inf:
                temp_file = manifest + '_populated'
                with open(temp_file, 'w') as outf:
                    tsv_reader = csv.DictReader(inf, delimiter='\t')
                    fieldnames = tsv_reader.fieldnames
                    fieldnames += MANIFEST_FIELDS
                    tsv_writer = csv.DictWriter(outf, delimiter='\t', fieldnames=fieldnames)
                    tsv_writer.writeheader()
                    line_count = 1
                    for record in tsv_reader:
                        line_count += 1
                        file_name = record.get(FILE_NAME, None)
                        if file_name:
                            file_path = os.path.join(data_folder, file_name)
                            if not os.path.isfile(file_path):
                                log.error('Invalid data at line {} : File "{}" doesn\'t exist!'.format(line_count, file_path))
                                succeeded = False
                            else:
                                # Populate fields in record
                                populate_record(record, data_folder, bucket, s3_folder)
                                final_files.append(file_path)
                        else:
                            log.error('Invalid data at line {} : Empty file name'.format(line_count))
                            succeeded = False

                        case_id = record.get(CASE_ID, None)
                        if not case_id:
                            log.error('Invalid data at line {} : Empty case_id'.format(line_count))
                            succeeded = False
                        tsv_writer.writerow(record)
        except Exception as e:
            log.exception(e)
            succeeded = False

    if succeeded:
        os.remove(manifest)
        os.rename(temp_file, manifest)
    else:
        os.remove(temp_file)
    return final_files if succeeded else succeeded



# Find and process manifest files, return list of updated manifest files and a list of files that included in manifest files
def process_manifest(data_folder, bucket, s3_folder) -> [str]:
    results = {MANIFESTS: [], FILES: []}
    file_list = glob.glob('{}/*.txt'.format(data_folder))
    try:
        for manifest in file_list:
            final_files = populate_manifest(manifest, data_folder, bucket, s3_folder)
            if not final_files:
                log.warning('Populate manifest file "{}" failed!'.format(manifest))
                continue
            else:
                results[MANIFESTS].append(manifest)
                results[FILES] += final_files
                log.info('Populated manifest file "{}"!'.format(manifest))

    except Exception as e:
        log.exception(e)

    return results

# Get possible one level folder inside tar/zip file
def get_true_data_folder(folder):
    folder = folder.replace(r'/$', '')
    first_level_file_list = glob.glob('{}/*'.format(folder))
    result = folder
    if len(first_level_file_list) == 1:
        inside_folder = first_level_file_list[0]
        if os.path.isdir(inside_folder):
            result = inside_folder
    return result

def handler(event, context):
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            # Assume raw files will be in a sub-folder inside '/RAW'
            if not key.startswith(RAW_PREFIX):
                log.error('File is not in {} folder'.format(RAW_PREFIX))
                return -1
            final_path = os.path.dirname(key).replace(RAW_PREFIX, FINAL_PREFIX)
            org_file_name = os.path.basename(key)
            download_path = '/tmp/{}{}'.format(uuid.uuid4(), org_file_name)
            extracted_path = '/tmp/{}'.format(uuid.uuid4())
            if not download_extract_file(bucket, key, download_path, extracted_path):
                log.error('Download RAW file "{}" failed'.format(org_file_name))
                return -1
            manifest_results = process_manifest(get_true_data_folder(extracted_path), bucket, final_path)
            if not manifest_results[MANIFESTS]:
                log.error('Process manifest failed!')
                return -1
            if not upload_files(bucket, final_path, manifest_results[FILES] + manifest_results[MANIFESTS]):
                log.error('Upload extracted files failed!')
                return -1
            if not send_sqs_message(queue, bucket, final_path):
                log.error('Send SQS message failed!')
                return -1

    except Exception as e:
        log.exception(e)
