#!/usr/bin/env python3

import argparse
import csv
import os

import requests


from bento.common.utils import get_logger, get_uuid, LOG_PREFIX, UUID
from bento.common.s3 import S3Bucket
from glioma import Glioma

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'File_Copier'

'''
This script copies (stream in memory) files from an URL to specified S3 bucket

Inputs:
  pre-manifest: TSV file that contains all information of original files
  target bucket: 
'''

class FileCopier:
    adapter_attrs = ['load_file_info', 'clear_file_info', 'get_org_url', 'get_dest_key', 'get_org_md5']

    DEFAULT_ACL = "['Open']"
    GUID = 'GUID'
    MD5 = 'md5'
    SIZE = 'size'
    ACL = 'acl'
    URL = 'url'
    MANIFEST_FIELDS = [GUID, MD5, SIZE, ACL, URL]

    FILE_SIZE = "file_size"
    MD5_SUM = 'md5sum'
    FILE_STAT = 'file_status'
    FILE_LOC = 'file_locations'
    FILE_FORMAT = 'file_format'
    DATA_FIELDS = [UUID, FILE_SIZE, MD5_SUM, FILE_STAT, FILE_LOC, FILE_FORMAT, ACL]

    DEFAULT_STAT = 'uploaded'
    INDEXD_GUID_PREFIX = 'dg.4DFC/'
    INDEXD_MANIFEST_EXT = '.tsv'
    DOMAIN = 'caninecommons.cancer.gov'

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

    def get_indexd_manifest_name(self, file_name):
        folder = os.path.dirname(file_name)
        base_name = os.path.basename(file_name)
        name, _ = os.path.splitext(base_name)
        new_name = '{}_indexd{}'.format(name, self.INDEXD_MANIFEST_EXT)
        return os.path.join(folder, new_name)

    @staticmethod
    def get_s3_location(bucket, key):
        return "s3://{}/{}".format(bucket, key)

    @staticmethod
    def get_neo4j_manifest_name(file_name):
        folder = os.path.dirname(file_name)
        base_name = os.path.basename(file_name)
        name, ext = os.path.splitext(base_name)
        new_name = '{}_neo4j{}'.format(name, ext)
        return os.path.join(folder, new_name)

    def populate_indexd_record(self, record, file_size):
        record[self.SIZE] = file_size
        record[self.MD5] = self.adapter.get_org_md5()
        record[self.GUID] = '{}{}'.format(self.INDEXD_GUID_PREFIX, get_uuid(self.DOMAIN, "file", record[self.MD5]))
        record[self.ACL] = self.DEFAULT_ACL
        record[self.URL] = self.get_s3_location(self.bucket_name, self.adapter.get_dest_key())
        return record

    def populate_neo4j_record(self, record, file_size, key):
        record[self.FILE_SIZE] = file_size
        record[self.FILE_LOC] = self.get_s3_location(self.bucket_name, key)
        file_name = self.adapter.get_file_name()
        record[self.MD5_SUM] = self.adapter.get_org_md5()
        record[self.FILE_FORMAT] = (os.path.splitext(file_name)[1]).split('.')[1].lower()
        record[UUID] = get_uuid(self.DOMAIN, "file", record[self.MD5_SUM])
        record[self.FILE_STAT] = self.DEFAULT_STAT
        record[self.ACL] = self.DEFAULT_ACL
        return record

    def copy_all(self):
        with open(self.pre_manifest) as pre_m:
            reader = csv.DictReader(pre_m, delimiter='\t')
            indexd_manifest = self.get_indexd_manifest_name(self.pre_manifest)
            neo4j_manifest = self.get_neo4j_manifest_name(self.pre_manifest)

            with open(indexd_manifest, 'w', newline='\n') as indexd_f:
                indexd_writer = csv.DictWriter(indexd_f, delimiter='\t', fieldnames=self.MANIFEST_FIELDS)
                indexd_writer.writeheader()
                with open(neo4j_manifest, 'w', newline='\n') as neo4j_f:
                    fieldnames = reader.fieldnames
                    fieldnames += self.DATA_FIELDS
                    neo4j_writer = csv.DictWriter(neo4j_f, delimiter='\t', fieldnames=fieldnames)
                    neo4j_writer.writeheader()

                    self.files_processed = 0
                    self.files_skipped = 0
                    self.files_copied = 0
                    self.files_failed = 0
                    self.files_exist_at_dest = 0
                    self.files_not_found = 0
                    for i in range(self.skip):
                        next(reader)
                        self.files_skipped += 1

                    line_num = self.files_skipped + 1
                    for file_info in reader:
                        self.files_processed += 1
                        line_num += 1
                        self.adapter.clear_file_info()
                        self.adapter.load_file_info(file_info)
                        org_url = self.adapter.get_org_url()
                        key = self.adapter.get_dest_key()
                        org_md5 = self.adapter.get_org_md5()
                        try:
                            if self.copy_file(org_url, org_md5, key):
                            # if self.file_exist(org_url):
                                file_size = self.bucket.get_object_size(key)
                                indexd_record = {}
                                self.populate_indexd_record(indexd_record, file_size)
                                indexd_writer.writerow(indexd_record)
                                self.populate_neo4j_record(file_info, file_size, key)
                                neo4j_writer.writerow(file_info)
                        except Exception as e:
                            self.files_failed += 1
                            self.log.error(f'Line: {line_num} - Copying file {key} FAILED!')
                            self.log.debug(e)

                        if self.count > 0 and self.files_processed >= self.count:
                            break
                    if self.files_skipped > 0:
                        self.log.info(f'Files skipped: {self.files_skipped}')
                    self.log.info(f'Files processed: {self.files_processed}')
                    self.log.info(f'Files not found: {self.files_not_found}')
                    self.log.info(f'Files copied: {self.files_copied}')
                    self.log.info(f'Files exist at destination: {self.files_exist_at_dest}')
                    self.log.info(f'Files failed: {self.files_failed}')

    def copy_file(self, org_url, org_md5, key):
        self.log.info(f'Copying from {org_url} to s3://{self.bucket_name}/{key} ...')

        if org_md5:
            if self.bucket.same_file_exists_on_s3(key, org_md5):
                self.log.info(f'Same file exists at destination: "{key}"')
                self.files_exist_at_dest += 1
                return True
        with requests.get(org_url, stream=True) as r:
            if r.status_code >= 400:
                self.log.error(f'Http Error Code {r.status_code} for {org_url}')
                return False
                # raise Exception(f'Http Error Code {r.status_code} for {org_url}')

            self.bucket._upload_file_obj(key, r.raw)
            self.files_copied += 1
            self.log.info(f'Copying file {key} SUCCEEDED!')
            return True

    def file_exist(self, org_url):
        self.log.info(f'Checking file {org_url}')
        with requests.head(org_url) as r:
            if r.ok:
                self.log.info(f'File exists!')
                return True
            elif r.status_code == 404:
                self.log.error(f'File not found!')
                self.files_not_found += 1
            else:
                self.log.error(r.status_code)
            return False


def main():
    parser = argparse.ArgumentParser(description='Copy files from orginal S3 buckets to specified bucket')
    parser.add_argument('-b', '--bucket', help='Destination bucket name', required=True)
    parser.add_argument('-p', '--prefix', help='Destination prefix for files', required=True)
    parser.add_argument('-f', '--first', help='First line to load, 1 based not counting headers', default=1, type=int)
    parser.add_argument('-c', '--count', help='number of files to copy, default is -1 means all files in the file', default=-1, type=int)
    parser.add_argument('pre_manifest', help='Pre-manifest file')
    args = parser.parse_args()

    copier = FileCopier(args.bucket, args.pre_manifest, args.first,  args.count, Glioma(args.prefix))
    copier.copy_all()

if __name__ == '__main__':
    main()
