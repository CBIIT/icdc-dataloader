#!/usr/bin/env python3

import argparse
import csv
import os

import requests


from bento.common.utils import get_logger, LOG_PREFIX
from bento.common.s3 import S3Bucket
from glioma import Glioma

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'File_Copier'

'''
This script moves files from S3 bucket(s) to specified S3 bucket

Inputs:
  pre-manifest: TSV file that contains all information of original files
  target bucket: 
'''

class FileCopier:
    adapter_attrs = ['get_s3_path']

    def __init__(self, bucket_name, pre_manifest, first, count, adapter):
        '''

        :param bucket_name: string type
        :param pre_manifest: string type, holds path to pre-manifest
        :param adapter: any object that has following methods/properties defined in adapter_attrs

        '''
        if not bucket_name:
            raise ValueError('Empty destination bucket name')
        self.bucket_name = bucket_name
        if not pre_manifest or not os.path.isfile(pre_manifest):
            raise ValueError(f'Pre-manifest: "{pre_manifest}" dosen\'t exist')
        self.pre_manifest = pre_manifest
        for attr in self.adapter_attrs:
            if not hasattr(adapter, attr):
                raise TypeError(f'Adapter doesn\'t have "{attr}" attribute/method')
        self.adapter = adapter

        if not first > 0 or count == 0:
            raise ValueError(f'Invalid first ({first}) or count ({count})')
        self.skip = first -1
        self.count = count
        self.bucket = S3Bucket(self.bucket_name)
        self.log = get_logger('FileCopier')

    def copy(self):
        with open(self.pre_manifest) as pre_m:
            reader = csv.DictReader(pre_m, delimiter='\t')
            files_processed = 0
            files_skipped = 0
            files_copied = 0
            files_failed = 0
            for i in range(self.skip):
                next(reader)
                files_skipped += 1

            line_num = files_skipped + 1
            for file_info in reader:
                files_processed += 1
                line_num += 1
                self.adapter.load_file_info(file_info)
                s3_path = self.adapter.get_s3_path()
                key = self.adapter.get_dest_key()
                org_md5 = self.adapter.get_org_md5()
                try:
                    self.log.info(f'Copying from {s3_path} to s3://{self.bucket_name}/{key} ...')
                    if org_md5:
                        if self.bucket.same_file_exists_on_s3(key, org_md5):
                            self.log.info(f'Same file exists at destination: "{key}"')
                            continue
                    with requests.get(s3_path, stream=True) as r:
                        if r.status_code >= 400:
                            raise Exception(f'Http Error Code {r.status_code} for {s3_path}')

                        self.bucket._upload_file_obj(key, r.raw)
                        files_copied += 1
                        self.log.info(f'Copying file {key} SUCCEEDED!')

                except Exception as e:
                    files_failed += 1
                    self.log.error(f'Line: {line_num} - Copying file {key} FAILED!')
                    self.log.debug(e)

                if self.count > 0 and files_processed >= self.count:
                    break
            if files_skipped > 0:
                self.log.info(f'Files skipped: {files_skipped}')
            self.log.info(f'Files processed: {files_processed}')
            self.log.info(f'Files copied: {files_copied}')
            self.log.info(f'Files failed: {files_failed}')


def main():
    parser = argparse.ArgumentParser(description='Copy files from orginal S3 buckets to specified bucket')
    parser.add_argument('-b', '--bucket', help='Destination bucket name', required=True)
    parser.add_argument('-p', '--prefix', help='Destination prefix for files', required=True)
    parser.add_argument('-f', '--first', help='First line to load, 1 based not counting headers', default=1, type=int)
    parser.add_argument('-c', '--count', help='number of files to copy, default is -1 means all files in the file', default=-1, type=int)
    parser.add_argument('pre_manifest', help='Pre-manifest file')
    args = parser.parse_args()

    copier = FileCopier(args.bucket, args.pre_manifest, args.first,  args.count, Glioma(args.prefix))
    copier.copy()

if __name__ == '__main__':
    main()
