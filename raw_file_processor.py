#!/usr/bin/env python3
# This file is used to unzip/untar raw files, populate manifest then call data loader
# It will listen on SQS for incoming messages sent by S3, to process new files

import boto3
from botocore.exceptions import ClientError
import uuid
import re
from urllib.parse import unquote_plus
import logging
import zipfile
import tarfile
import glob
import os, sys
from utils import *
from sqs import Queue
import json
import csv
import hashlib
import io
import argparse
from sqs import *
import re
import shutil
from data_loader import DataLoader
import neo4j
from icdc_schema import ICDC_Schema
from timeit import default_timer as timer

RAW_PREFIX = 'RAW'
FINAL_PREFIX = 'Final'
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
RECORDS = 'Records'
END_NORMALLY = 'end_normally'
MESSAGE = 'message'
FILES = 'files'

class FileProcessor:
    def __init__(self, queue_name, driver, schema, manifest_bucket, manifest_folder, dry_run=False):
        self.log = get_logger('File Processor')
        self.queue_name = queue_name
        self.s3_client = boto3.client('s3')
        if not isinstance(driver, neo4j.Driver):
            raise Exception('Neo4j driver is invalid!')
        self.driver = driver
        if not isinstance(schema, ICDC_Schema):
            raise Exception('Scheme is invalid!')
        self.schema = schema
        if not manifest_bucket:
            raise Exception('Manifest bucket is invalid!')
        self.manifest_bucket = manifest_bucket
        if not manifest_folder:
            raise Exception('Manifest folder is invalid')
        self.manifest_folder = manifest_folder
        self.dry_run = dry_run

    @staticmethod
    def join_path(*args):
        if len(args) == 0:
            return ''

        result = ''
        for index in range(0, len(args)):
            part = re.sub(r'/+$', '', args[index])
            if index != 0:
                part = re.sub(r'^/+', '', part)
            if result:
                result = '{}/{}'.format(result, part)
            else:
                result = part
        return result

    def get_md5(self, file_name):
        hash = hashlib.md5()
        with open(file_name, 'rb') as afile:
            buf = afile.read(BLOCK_SIZE)
            while len(buf) > 0:
                hash.update(buf)
                buf = afile.read(BLOCK_SIZE)
        return hash.hexdigest()

    def upload_extracted_file(self, file_name, local_folder, bucket, final_path, files):
        local_file = os.path.join(local_folder, file_name)
        s3_file_path = self.join_path(final_path, file_name)
        md5 = self.get_md5(local_file)
        try:
            self.s3_client.head_object(Bucket=bucket, Key=s3_file_path, IfMatch=md5)
            self.log.info('Skipped file {} - Same file already exists on S3'.format(s3_file_path))
            files[file_name] = {FILE_NAME: file_name,
                                FILE_LOC: self.get_s3_location(bucket, final_path, file_name),
                                FILE_SIZE: os.stat(local_file).st_size,
                                MD5: md5}
        except ClientError as e:
            if e.response['Error']['Code'] in ['404', '412']:
                with open(local_file, 'rb') as lf:
                    s3_obj = self.s3_client.put_object(Bucket=bucket, Key=s3_file_path, Body=lf)
                    files[file_name] = {FILE_NAME: file_name,
                                        FILE_LOC: self.get_s3_location(bucket, final_path, file_name),
                                        FILE_SIZE: os.stat(local_file).st_size,
                                        MD5: s3_obj['ETag'].replace('"', '')}
            else:
                self.log.error('Unknown S3 client error!')
                self.log.exception(e)
        if not file_name.endswith('.txt'):
            os.remove(local_file)

    # Download and extract files, return list of files with final location and md5
    def extract_file(self, bucket, key, final_path, local_folder):
        result = {END_NORMALLY: True}
        try:
            parts = key.split('.')
            ext = parts[-1]
            if len(parts) > 1:
                if ext == 'zip':
                    local_zip_file = os.path.join(local_folder, os.path.basename(key))
                    files = {}
                    self.log.info('downloading zip file: {}'.format(key))
                    self.s3_client.download_file(bucket, key, local_zip_file)
                    with zipfile.ZipFile(local_zip_file, "r") as zip_ref:
                        self.log.info('Extracting zip file: {}'.format(key))
                        for item in zip_ref.infolist():
                            file_name = item.filename
                            file_path = self.join_path(final_path, file_name)
                            if not item.is_dir() and '/' not in file_name:
                                self.log.info('Extracting {} to {}'.format(file_name, file_path))
                                zip_ref.extract(item, local_folder)
                                self.upload_extracted_file(file_name, local_folder, bucket, final_path, files)
                    result[FILES] = files
                    return result

                if ext == 'tar':
                    self.log.info('Streaming tar file: {}'.format(key))
                    files = {}
                    obj = self.s3_client.get_object(Bucket=bucket, Key=key)
                    stream = io.BufferedReader(obj['Body']._raw_stream)
                    with tarfile.open(None, 'r|*', stream) as tar_ref:
                        self.log.info('Extracting tar file: {}'.format(key))
                        for item in tar_ref:
                            file_name = item.name
                            file_path = self.join_path(final_path, file_name)
                            if item.isfile() and '/' not in file_name:
                                self.log.info('Extracting {} to {}'.format(file_name, file_path))
                                tar_ref.extract(item, local_folder)
                                self.upload_extracted_file(file_name, local_folder, bucket, final_path, files)
                    result[FILES] = files
                    return result
                else:
                    msg = '{} file is not supported!'.format(ext)
                    self.log.warning(msg)
                    result[MESSAGE] = msg
                    return result
            else:
                msg = '{} file is not supported!'.format(ext)
                self.log.warning(msg)
                result[MESSAGE] = msg
                return result

        except Exception as e:
            self.log.exception(e)
            result[END_NORMALLY] = False
            result[MESSAGE] = str(e)
            return result

    def upload_manifests(self, folder, file_list):
        try:
            for file in file_list:
                if os.path.isfile(file):
                    self.log.info('Uploading manifest: {}'.format(file))
                    file_name = self.join_path(self.manifest_folder, folder, os.path.basename(file))
                    self.s3_client.upload_file(file, self.manifest_bucket, file_name)
                else:
                    self.log.info('{} is not a file, and won\'t be uploaded to S3'.format(file))
            return True
        except Exception as e:
            self.log.exception(e)
            return False


    def send_sqs_message(self, queue, data_bucket, data_path):
        try:
            obj = {
                'type': ICDC_FILE_UPLOADED,
                'bucket': data_bucket,
                'path': data_path
            }
            queue.sendMsgToQueue(json.dumps(obj), data_path)
            self.log.info('Data path: {}'.format(data_path))
            return True
        except Exception as e:
            self.log.exception(e)
            return False

    @staticmethod
    def get_s3_location(bucket, folder, key):
        return "s3://{}/{}/{}".format(bucket, folder, key)

    def populate_record(self, record, file_info):
        file_name = file_info[FILE_NAME]
        record[FILE_SIZE] = file_info[FILE_SIZE]
        record[FILE_LOC] = file_info[FILE_LOC]
        record[MD5] = file_info[MD5]
        record[FILE_FORMAT] = (os.path.splitext(file_name)[1]).split('.')[1].lower()
        record[UUID] = get_uuid_for_node("file", record[FILE_LOC])
        record[FILE_STAT] = DEFAULT_STAT
        record[ACL] = DEFAULT_ACL
        return record

    # check the field file_name/case id in the manifest which should not be null/empty
    # check files included in the manifest exist or not
    def populate_manifest(self, manifest, extracted_files):
        self.log.info('Validating manifest: {}'.format(manifest))
        succeeded = True
        # check manifest
        if not os.path.isfile(manifest):
            self.log.error('Manifest: "{}" does not exists !'.format(manifest))
            succeeded = False
        else:
            try:
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
                                if not file_name in extracted_files:
                                    self.log.error('Invalid data at line {} : File "{}" doesn\'t exist!'.format(line_count, file_name))
                                    succeeded = False
                                else:
                                    # Populate fields in record
                                    self.populate_record(record, extracted_files[file_name])
                            else:
                                self.log.error('Invalid data at line {} : Empty file name'.format(line_count))
                                succeeded = False

                            case_id = record.get(CASE_ID, None)
                            if not case_id:
                                self.log.error('Invalid data at line {} : Empty case_id'.format(line_count))
                                succeeded = False
                            tsv_writer.writerow(record)
            except Exception as e:
                self.log.exception(e)
                succeeded = False

        if succeeded:
            os.remove(manifest)
            os.rename(temp_file, manifest)
        else:
            os.remove(temp_file)
        return succeeded



    # Find and process manifest files, return list of updated manifest files and a list of files that included in manifest files
    def process_manifest(self, data_folder, extracted_files) -> [str]:
        results = []
        file_list = glob.glob('{}/*.txt'.format(data_folder))
        if not file_list:
            self.log.error('No manifest (TSV/TXT) files found!')
            return results
        try:
            for manifest in file_list:
                if not self.populate_manifest(manifest, extracted_files):
                    self.log.warning('Populate manifest file "{}" failed!'.format(manifest))
                    continue
                else:
                    results.append(manifest)
                    self.log.info('Populated manifest file "{}"!'.format(manifest))

        except Exception as e:
            self.log.exception(e)

        return results

    # Get possible one level folder inside tar/zip file
    def get_true_data_folder(self, folder):
        folder = folder.replace(r'/$', '')
        first_level_file_list = glob.glob('{}/*'.format(folder))
        result = folder
        if len(first_level_file_list) == 1:
            inside_folder = first_level_file_list[0]
            if os.path.isdir(inside_folder):
                result = inside_folder
        return result

    def is_file(self, bucket, key):
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            self.log.info('Skipped file {} - Same file already exists on S3'.format(s3_file_path))
            files[file_name] = {FILE_NAME: file_name,
                                FILE_LOC: self.get_s3_location(bucket, final_path, file_name),
                                FILE_SIZE: os.stat(local_file).st_size,
                                MD5: md5}
        except ClientError as e:
            if e.response['Error']['Code'] in ['404', '412']:
                with open(local_file, 'rb') as lf:
                    s3_obj = self.s3_client.put_object(Bucket=bucket, Key=s3_file_path, Body=lf)
                    files[file_name] = {FILE_NAME: file_name,
                                        FILE_LOC: self.get_s3_location(bucket, final_path, file_name),
                                        FILE_SIZE: os.stat(local_file).st_size,
                                        MD5: s3_obj['ETag'].replace('"', '')}
            else:
                self.log.error('Unknown S3 client error!')
                self.log.exception(e)

    def handler(self, event):
        succeeded = True
        for record in event['Records']:
            start = timer()
            end = start
            temp_folder = os.path.join(TEMP_FOLDER, str(uuid.uuid4()))
            try:
                os.makedirs(temp_folder)
                bucket = record['s3']['bucket']['name']
                key = unquote_plus(record['s3']['object']['key'])
                size = int(record['s3']['object']['size'])
                # Assume raw files will be in a sub-folder inside '/RAW'
                if not key.startswith(RAW_PREFIX):
                    self.log.warning('File is not in {} folder'.format(RAW_PREFIX))
                    continue

                if size == 0:
                    self.log.warning('{} is not a file'.format(key))
                    continue

                final_path = os.path.dirname(key).replace(RAW_PREFIX, FINAL_PREFIX)
                extract_result = self.extract_file(bucket, key, final_path, temp_folder)
                file_list = extract_result.get(FILES, {})
                if not extract_result[END_NORMALLY]:
                    msg = 'Extract RAW file "{}" failed'.format(key)
                    self.log.error(msg)
                    self.send_failure_email(msg)
                    succeeded = False
                    continue
                if file_list:
                    manifests = self.process_manifest(temp_folder, file_list)
                    if not manifests:
                        self.log.error('Process manifest failed!')
                        self.send_failure_email('Process manifest failed!')
                        continue
                    manifest_folder = os.path.dirname(key).replace(RAW_PREFIX, '')
                    self.upload_manifests(manifest_folder, manifests)
                    loading_result = self.load_manifests(manifests)
                    end = timer()
                    if loading_result:
                        self.send_success_email(key, final_path, file_list, [os.path.basename(x) for x in manifests], loading_result, end - start)
                    else:
                        self.log.error('Load manifests failed!')
                        self.send_failure_email('Load manifests failed!')
                # Todo: do I need to do something here
            except Exception as e:
                end = timer()
                self.log.exception(e)
                self.send_failure_email(e)
                return False

            finally:
                shutil.rmtree(temp_folder)
                end = timer()
                self.log.info('Running time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
        return succeeded


    def send_success_email(self, file_name, final_path, file_list, manifests, loading_result, running_time):
        content = 'S3 file processing succeeded!<br>\n'
        content += 'File processed: {}<br>\n'.format(file_name)
        content += 'Files extracted and uploaded to {}:<br>\n'.format(final_path)
        content += '<br>\n'.join(file_list) + '<br>\n'
        content += '=' * 70 + '<br>\n'
        content += 'Manifests processed:<br>\n'
        content += '<br>\n'.join(manifests) + '<br>\n'
        if loading_result:
            content += '=' * 70 + '<br>\n'
            content += 'File nodes created: {}<br>\n'.format(loading_result[NODES_CREATED])
            content += 'Relationships created: {}<br>\n'.format(loading_result[RELATIONSHIP_CREATED])
        content += 'Running time: {:.2f} seconds<br>\n'.format(running_time)

        self.log.info('Sending success email...')
        send_mail('S3 File Processing Succeeded!', content)
        self.log.info('Success email sent')

    def send_failure_email(self, message):
        content = str(message)
        self.log.info('Sending failure email...')
        send_mail('S3 File Processing FAILED!', content)
        self.log.info('Failure email sent')

    def listen(self):
        self.queue = Queue(self.queue_name)
        self.log.info('PIMixture Processor service started!')
        while True:
            self.log.info("Receiving more messages...")
            for msg in self.queue.receiveMsgs(VISIBILITY_TIMEOUT):
                extender = None
                try:
                    data = json.loads(msg.body)
                    if data and RECORDS in data and isinstance(data[RECORDS], list):
                        extender = VisibilityExtender(msg, VISIBILITY_TIMEOUT)
                        self.log.info('Start processing job ...')

                        if self.handler(data):
                            self.log.info('Finish processing job!')
                            msg.delete()
                        else:
                            self.log.info('Processing job failed!')
                        extender.stop()
                        extender = None

                except Exception as e:
                    self.log.exception(e)
                    self.send_failure_email('S3 File Processing FAILED!\n' + str(e))

                finally:
                    if extender:
                        extender.stop()
                        extender = None

    def load_manifests(self, manifests):
        try:
            self.loader = DataLoader(self.driver, self.schema, manifests)
            if isinstance(self.loader, DataLoader):
                for file in manifests:
                    if not self.loader.validate_cases_exist_in_file(file, 1):
                        self.log.error('Validate parents in {} failed, abort loading!'.format(file))
                        return False
                return self.loader.load(False, self.dry_run, 1)
            else:
                self.log.error('Can\'t load manifest, because data loader is not valid!')
                return False
        except Exception as e:
            self.log.exception(e)
            return False




def main(args):
    log = get_logger('Raw file processor - main')

    if not args.queue:
        log.error('Please specify queue name with -q/--queue argument')
        sys.exit(1)

    uri = args.uri if args.uri else "bolt://localhost:7687"
    uri = removeTrailingSlash(uri)

    password = args.password
    if not password:
        if PSWD_ENV not in os.environ:
            log.error(
                'Password not specified! Please specify password with -p or --password argument, or set {} env var'.format( PSWD_ENV))
            sys.exit(1)
        else:
            password = os.environ[PSWD_ENV]
    user = args.user if args.user else 'neo4j'

    if not args.schema:
        log.error('Please specify schema file(s) with -s or --schema argument')
        sys.exit(1)

    for schema_file in args.schema:
        if not os.path.isfile(schema_file):
            log.error('{} is not a file'.format(schema_file))
            sys.exit(1)

    if not args.bucket:
        log.error('Please specify output S3 bucket for final manifest(s) using -b/--bucket argument')
        sys.exit(1)

    if not args.s3_folder:
        log.error('Please specify output S3 folder for final manifest(s) using -f/--s3-folder argument')
        sys.exit(1)

    driver = None
    try:
        schema = ICDC_Schema(args.schema)
        driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))
        processor = FileProcessor(args.queue, driver, schema, args.bucket, args.s3_folder, args.dry_run)
        processor.listen()

    except neo4j.ServiceUnavailable as err:
        log.exception(err)
        log.critical("Can't connect to Neo4j server at: \"{}\"".format(uri))

    except KeyboardInterrupt:
        log.info("\nBye!")
        sys.exit()

    finally:
        if driver:
            driver.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process incoming S3 files and call data loader to load into Neo4j')
    parser.add_argument('-q', '--queue', help='SQS queue name')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('-d', '--dry-run', help='Validations only, skip loading', action='store_true')
    parser.add_argument('-m', '--max-violations', help='Max violations to display', nargs='?', type=int, default=10)
    parser.add_argument('-b', '--bucket', help='Output (manifest) S3 bucket name')
    parser.add_argument('-f', '--s3-folder', help='Output (manifest) S3 folder')
    args = parser.parse_args()
    main(args)