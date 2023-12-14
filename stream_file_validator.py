#from config import BentoConfig
import argparse
import csv
from data_loader import check_encoding
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
import pandas as pd
import boto3
import hashlib
import os
import sys
import yaml

FILE_URL = 'file_url'
BUCKET = 'bucket'
PREFIX = 'prefix'
FILE_NAME = "file_name"
FILE_SIZE = "file_size"
FILE_MD5 = "file_md5"
VALIDATION_RESULT = "validation_result"
VALIDATION_FAIL_REASON = "validation_fail_reason"
MANIFEST_FILE = 'manifest_file'
OUTPUT_FILE = 'output_file_location'
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Stream_File_Validator'

os.environ[APP_NAME] = 'Stream_File_Validator'

class BentoConfig:
    def __init__(self, config_file, args, config_file_arg='config_file'):
        self.log = get_logger('Bento Config')
        self.data = {}

        self.config_file_arg = config_file_arg
        if config_file:
            with open(config_file) as c_file:
                self.data = yaml.safe_load(c_file)['Config']
                if self.data is None:
                    self.data = {}

        self._override(args)

    def _override(self, args):
        for key, value in vars(args).items():
            # Ignore config file argument
            if key == self.config_file_arg:
                continue
            if isinstance(value, bool):
                if value:
                    self.data[key] = value

            elif value is not None:
                self.data[key] = value



def process_arguments(args, log):
    config_file = None
    if args.config_file:
        config_file = args.config_file
    config = BentoConfig(config_file, args)
    print(config)
    return config

def parse_arguments():
    parser = argparse.ArgumentParser(description='Validate files through streming files from the s3 bucket')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--manifest_file', help='manifest_file location')
    parser.add_argument('--file_name', help='The file name column in the input file')
    parser.add_argument('--file_url', help='The file s3 url column in the input file')
    parser.add_argument('--file_size', help='The file size column in the input file')
    parser.add_argument('--file_md5', help='The file md5 column in the input file')
    parser.add_argument('--bucket', help='The s3 bucket of the uploaded file')
    parser.add_argument('--prefix', help="The s3 bucket's subfolder of the uploaded file")
    parser.add_argument('--output_file_location', help="The output validation result file's location")
    return parser.parse_args()

class SteamfileValidator():
    def __init__(self, config_data):
        self.manifest_file = config_data[MANIFEST_FILE]
        self.log = get_logger('Stream File Validator')
        self.file_name = config_data[FILE_NAME]
        self.file_size = config_data[FILE_SIZE]
        self.file_md5 = config_data[FILE_MD5]
        self.output_file_location = config_data[OUTPUT_FILE]
        self.s3_client = boto3.client('s3')
        if FILE_URL in config_data.keys():
            self.file_url = config_data[FILE_URL]
        else:
            self.file_url = None
        if BUCKET in config_data.keys():
            self.bucket = config_data[BUCKET]
        else:
            self.bucket = None
        if PREFIX in config_data.keys():
            self.prefix = config_data[PREFIX]
        else:
            self.prefix = None
    def s3_url_transform(self, s3_url):
        s3_url_list = s3_url.split('/')
        s3_bucket = s3_url_list [2]
        s3_file_key = ""
        for i in range(3, len(s3_url_list)):
            if i != len(s3_url_list) - 1:
                s3_file_key = s3_file_key + s3_url_list[i] + "/"
            else:
                s3_file_key = s3_file_key + s3_url_list[i]
        return s3_bucket, s3_file_key
    def check_existence(self, s3_bucket, s3_file_key):
        try:
            response = self.s3_client.head_object(Bucket=s3_bucket, Key=s3_file_key)
            return True
        except Exception as e:
            self.log.error(e)
            return False
    def check_file_size(self, s3_bucket, s3_file_key, file_size):
        response = self.s3_client.head_object(Bucket=s3_bucket, Key=s3_file_key)
        file_size = int(file_size)
        if file_size == response['ContentLength']:
            return True
        else:
            return False
        
    def check_md5sum(self, s3_bucket, s3_file_key, md5):
        s3_file = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
        s3_file_content = s3_file['Body'].read()
        s3_hash = hashlib.md5(s3_file_content).hexdigest()
        if s3_hash == md5:
            return True
        else:
            return False
    def get_s3_file_information(self, org_obj):
        s3_bucket = self.bucket
        if self.prefix is not None:
            s3_file_key = os.path.join(self.prefix, org_obj[self.file_name])
        else:
            s3_file_key = org_obj[self.file_name]
        return s3_bucket, s3_file_key

    def validate_stream_file(self):
        validation_df = pd.DataFrame(columns=[FILE_NAME, FILE_URL, FILE_SIZE, FILE_MD5, VALIDATION_RESULT, VALIDATION_FAIL_REASON])
        file_encoding = check_encoding(self.manifest_file)
        with open(self.manifest_file, encoding=file_encoding) as in_file:
            manifest = csv.DictReader(in_file, delimiter='\t')
            line_number = 1
            for org_obj in manifest:
                line_number += 1
                s3_bucket = ""
                s3_file_key = ""
                tmp_validation_df = pd.DataFrame()
                if self.file_url in org_obj.keys():
                    if org_obj[self.file_url] != "":
                        s3_bucket, s3_file_key = self.s3_url_transform(org_obj[self.file_url])
                    else:
                        s3_bucket, s3_file_key = self.get_s3_file_information(org_obj)
                elif self.bucket is not None:
                    s3_bucket, s3_file_key = self.get_s3_file_information(org_obj)
                else:
                    self.log.error("If file urls are not available in the manifest, then bucket name and prefix (folder name) need to be provided, abort validation")
                    sys.exit(1)
                validation_fail_reason = []
                tmp_validation_df[VALIDATION_RESULT] = ['passed']
                if org_obj[self.file_url] == "":
                        tmp_validation_df[VALIDATION_RESULT] = ['warning']
                        self.log.warning(f'{self.file_url} missing at line {line_number}')
                        validation_fail_reason.append(f"{self.file_url}_missing")
                if org_obj[self.file_name] == "":
                    tmp_validation_df[VALIDATION_RESULT] = ['warning']
                    self.log.warning(f'{self.file_name} missing at line {line_number}')
                    validation_fail_reason.append(f"{self.file_name}_missing")
                file_exist = self.check_existence(s3_bucket, s3_file_key)
                if file_exist:
                    if org_obj[self.file_size] == "":
                        tmp_validation_df[VALIDATION_RESULT] = ['error']
                        self.log.error(f'{self.file_size} missing at line {line_number}')
                        validation_fail_reason.append(f"{self.file_size}_missing")
                    else:
                        if not self.check_file_size(s3_bucket, s3_file_key, org_obj[self.file_size]):
                            tmp_validation_df[VALIDATION_RESULT] = ['failed']
                            validation_fail_reason.append('file_size_validation_fail')
                            self.log.error(f"file size validation fail at line {line_number}")
                    if org_obj[self.file_md5] == "":
                        tmp_validation_df[VALIDATION_RESULT] = ['error']
                        self.log.error(f'{self.file_md5} missing at line {line_number}')
                        validation_fail_reason.append(f"{self.file_md5}_missing")
                    else:
                        if not self.check_md5sum(s3_bucket, s3_file_key, org_obj[self.file_md5]):
                            tmp_validation_df[VALIDATION_RESULT] = ['failed']
                            validation_fail_reason.append('file_md5_validation_fail')
                            self.log.error(f"file md5 validation fail at line {line_number}")
                else:
                    tmp_validation_df[VALIDATION_RESULT] = ['failed']
                    #tmp_validation_df[VALIDATION_FAIL_REASON] = ['file_not_exist_in_s3']
                    validation_fail_reason.append('file_not_exist_in_s3')
                tmp_validation_df[FILE_NAME] = org_obj[self.file_name]
                tmp_validation_df[FILE_URL] = org_obj[self.file_url]
                tmp_validation_df[FILE_SIZE] = org_obj[self.file_size]
                tmp_validation_df[FILE_MD5] = org_obj[self.file_md5]
                validation_fail_reason_str = ""
                if len(validation_fail_reason) > 0:
                    for i in range(0, len(validation_fail_reason)):
                        if i == 0:
                            validation_fail_reason_str = validation_fail_reason[i]
                        else:
                            validation_fail_reason_str = validation_fail_reason_str + "," + validation_fail_reason[i]
                    tmp_validation_df[VALIDATION_FAIL_REASON] = [validation_fail_reason_str]
                validation_df = pd.concat([validation_df, tmp_validation_df])

            validation_file_key = os.path.join(self.output_file_location, os.path.basename(self.manifest_file).replace(".txt", "_validation_result.tsv"))
            validation_df.to_csv(validation_file_key, sep="\t", index=False) 
                    

            

def main(args):
    log = get_logger('Stream File Validator')
    config = process_arguments(args, log)
    stream_file_validator = SteamfileValidator(config.data)
    stream_file_validator.validate_stream_file()
    
    #with open(config.manifest_file, encoding=file_encoding) as in_file
    

if __name__ == '__main__':
    main(parse_arguments())
