#from config import BentoConfig
import argparse
import zipfile
import pandas as pd
import boto3
import os
import sys
import yaml
from urllib.parse import urlparse
from bento.common.s3 import upload_log_file
from bento.common.utils import get_time_stamp, get_log_file
from bento.common.utils import get_logger, get_stream_md5, LOG_PREFIX, APP_NAME

FILE_URL_COLUMN = 'file_url_column'
VALIDATION_S3_BUCKET = 'validation_s3_bucket'
VALIDATION_PREFIX = 'validation_prefix'
FILE_NAME_COLUMN = "file_name_column"
FILE_SIZE_COLUMN = "file_size_column"
FILE_MD5_COLUMN = "file_md5_column"
VALIDATION_RESULT = "validation_result"
VALIDATION_FAIL_REASON = "validation_fail_reason"
MANIFEST_FILE = 'manifest_file'
TEMP_FOLDER = 'tmp'
UPLOAD_S3_URL = "upload_s3_url"
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
    argument_list = [MANIFEST_FILE, FILE_NAME_COLUMN, FILE_SIZE_COLUMN, FILE_MD5_COLUMN]
    check_argument(config, argument_list, log)
    return config

def check_argument(config, argument_list, log):
    for argument in argument_list:
        if argument not in config.data.keys():
            log.error(f'The argument {argument} is invalid!')
            sys.exit(1)
        else:
            if config.data[argument] is None:
                log.error(f'The argument {argument} is invalid!')
                sys.exit(1)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Validate files through streming files from the s3 bucket')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--manifest-file', help='manifest file location')
    parser.add_argument('--file-name-column', help='The file name column in the input file')
    parser.add_argument('--file-url-column', help='The file s3 url column in the input file')
    parser.add_argument('--file-size-column', help='The file size column in the input file')
    parser.add_argument('--file-md5-column', help='The file md5 column in the input file')
    parser.add_argument('--validation-s3-bucket', help='The s3 bucket of the uploaded file')
    parser.add_argument('--validation-prefix', help="The s3 bucket's subfolder of the uploaded file")
    parser.add_argument('--upload-s3-url', help='upload s3 file location')
    return parser.parse_args()

class SteamfileValidator():
    def __init__(self, config_data):
        self.manifest_file= config_data[MANIFEST_FILE]
        self.log = get_logger('Stream File Validator')
        self.file_name_column = config_data[FILE_NAME_COLUMN]
        self.file_size_column = config_data[FILE_SIZE_COLUMN]
        self.file_md5_column = config_data[FILE_MD5_COLUMN]
        self.output_folder = TEMP_FOLDER
        self.download_from_s3 = False
        self.s3_client = boto3.client('s3')
        if UPLOAD_S3_URL in config_data.keys():
            self.upload_s3_url = config_data[UPLOAD_S3_URL]
        else:
            self.upload_s3_url = None
        if FILE_URL_COLUMN in config_data.keys():
            self.file_url_column = config_data[FILE_URL_COLUMN]
        else:
            self.file_url_column = None
        if VALIDATION_S3_BUCKET in config_data.keys():
            self.validation_s3_bucket = config_data[VALIDATION_S3_BUCKET]
        else:
            self.validation_s3_bucket = None
        if VALIDATION_PREFIX in config_data.keys():
            self.validation_prefix = config_data[VALIDATION_PREFIX]
        else:
            self.validation_prefix = None
    def s3_url_transform(self, url):
        s3_url = urlparse(url)
        if s3_url.scheme != 's3':
            return None, None
        s3_bucket = s3_url.netloc
        s3_file_key = s3_url.path.lstrip('/')
        return s3_bucket, s3_file_key
    
    def check_existence(self, s3_bucket, s3_file_key):
        try:
            response = self.s3_client.head_object(Bucket=s3_bucket, Key=s3_file_key)
            error_code = None
            return True, error_code
        except Exception as e:
            self.log.error(e)
            try:
                error_code = e.response['Error']['Code']
            except Exception as e:
                error_code = "Other"
            return False, error_code
    def check_file_size(self, s3_bucket, s3_file_key, file_size):
        response = self.s3_client.head_object(Bucket=s3_bucket, Key=s3_file_key)
        file_size = int(file_size)
        if file_size == response['ContentLength']:
            return True
        else:
            return False
        
    def check_md5sum(self, s3_bucket, s3_file_key, md5):
        s3_file = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
        s3_file_stream = s3_file['Body']
        s3_hash = get_stream_md5(s3_file_stream)
        if s3_hash == md5:
            return True
        else:
            return False

    def get_s3_file_information(self, org_obj):
        s3_bucket = self.validation_s3_bucket
        if self.validation_prefix is not None:
            s3_file_key = os.path.join(self.validation_prefix, org_obj[self.file_name_column])
        else:
            s3_file_key = org_obj[self.file_name_column]
        return s3_bucket, s3_file_key

    def read_s3_csv_file(self):
        s3_url = urlparse(self.manifest_file)
        s3_bucket = s3_url.netloc
        s3_file_key = s3_url.path.lstrip('/')
        s3_file = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
        s3_df = pd.read_csv(s3_file['Body'], sep='\t', na_values=[""])
        #Remove leading and trailing spaces
        s3_df.columns = s3_df.columns.str.strip()
        return s3_df

    def validate_stream_file(self):
        if self.file_url_column is None:
            validation_df = pd.DataFrame(columns=[self.file_name_column, self.file_size_column, self.file_md5_column, VALIDATION_RESULT, VALIDATION_FAIL_REASON])
            self.log.warning(f"The file url column is not configured")
        else:
            validation_df = pd.DataFrame(columns=[self.file_name_column, self.file_url_column, self.file_size_column, self.file_md5_column, VALIDATION_RESULT, VALIDATION_FAIL_REASON])
        if self.manifest_file.startswith("s3://"):
            #If start with s3://, then read the csv file from s3 bucket
            self.download_from_s3 = True
            try:
                manifest_df = self.read_s3_csv_file()
            except Exception as e:
                self.log.error(e)
                self.log.error("Uable to read the manifest file from s3, abort validation")
                sys.exit(1)
        else:
            #Remove leading and trailing spaces
            try:
                manifest_df = pd.read_csv(self.manifest_file, sep='\t', na_values=[""])
            except Exception as e:
                self.log.error(e)
                self.log.error("Uable to read the manifest file from local, abort validation")
                sys.exit(1)
            manifest_df.columns = manifest_df.columns.str.strip()

        manifest = manifest_df.to_dict(orient='records')
        line_number = 1
        # Get manifest file name
        if self.download_from_s3:
            s3_download_bucket, s3_download_file_key = self.s3_url_transform(self.manifest_file)
            s3_download_prefix = os.path.dirname(s3_download_file_key)
            if self.upload_s3_url is None:
                dest_log_dir = f's3://{s3_download_bucket}/{s3_download_prefix}'
            else:
                dest_log_dir = self.upload_s3_url
            manifest_file_name = os.path.basename(s3_download_file_key)
        else:
            manifest_file_name = os.path.basename(self.manifest_file)

        for org_obj in manifest:
            line_number += 1
            #If the given column names are incorrect
            if self.file_md5_column not in org_obj.keys():
                self.log.error(f"The file md5 column {self.file_md5_column} given can not be found in the manifest file {manifest_file_name}, abort validation")
                sys.exit(1)
            if self.file_name_column not in org_obj.keys():
                self.log.error(f"The file name column {self.file_name_column} given can not be found in the manifest file {manifest_file_name}, abort validation")
                sys.exit(1)
            if self.file_size_column not in org_obj.keys():
                self.log.error(f"The file size column {self.file_size_column} given can not be found in the manifest file {manifest_file_name}, abort validation")
                sys.exit(1)
            if self.file_url_column is not None and self.file_url_column not in org_obj.keys():
                self.log.error(f"The file url column {self.file_url_column} given can not be found in the manifest file {manifest_file_name}, abort validation")
                sys.exit(1)
            s3_bucket = ""
            s3_file_key = ""
            tmp_validation_df = pd.DataFrame()
            validation_fail_reason = []
            tmp_validation_df[VALIDATION_RESULT] = ['passed']
            if self.file_url_column in org_obj.keys():
                #If there is an file_url column
                if not pd.isna(org_obj[self.file_url_column]):
                    #If file_url value not empty
                    s3_bucket, s3_file_key = self.s3_url_transform(org_obj[self.file_url_column])
                    if s3_bucket is None and s3_file_key is None:
                        #If the url is not s3 url
                        self.log.error(f"The {self.file_url_column} value {org_obj[self.file_url_column]} is invalid at line {line_number}")
                        tmp_validation_df[VALIDATION_RESULT] = ['error']
                        validation_fail_reason.append(f"{self.file_url_column}_invalid")
                else:
                    s3_bucket, s3_file_key = self.get_s3_file_information(org_obj)
            elif self.validation_s3_bucket is not None:
                s3_bucket, s3_file_key = self.get_s3_file_information(org_obj)
            else:
                self.log.error("If file urls are not available in the manifest, then bucket name and prefix (folder name) need to be provided, abort validation")
                sys.exit(1)
            if self.file_url_column is not None:
                if pd.isna(org_obj[self.file_url_column]):
                        tmp_validation_df[VALIDATION_RESULT] = ['warning']
                        self.log.warning(f'{self.file_url_column} missing at line {line_number}')
                        validation_fail_reason.append(f"{self.file_url_column}_missing")
            if  pd.isna(org_obj[self.file_name_column]):
                tmp_validation_df[VALIDATION_RESULT] = ['warning']
                self.log.warning(f'{self.file_name_column} missing at line {line_number}')
                validation_fail_reason.append(f"{self.file_name_column}_missing")
            file_exist, error_code = self.check_existence(s3_bucket, s3_file_key)
            if file_exist:
                if pd.isna(org_obj[self.file_size_column]):
                    tmp_validation_df[VALIDATION_RESULT] = ['error']
                    self.log.error(f'{self.file_size_column} missing at line {line_number}')
                    validation_fail_reason.append(f"{self.file_size_column}_missing")
                else:
                    if not self.check_file_size(s3_bucket, s3_file_key, org_obj[self.file_size_column]):
                        tmp_validation_df[VALIDATION_RESULT] = ['failed']
                        validation_fail_reason.append('file_size_validation_fail')
                        self.log.error(f"file size validation fail at line {line_number}")
                if pd.isna(self.file_md5_column):
                    tmp_validation_df[VALIDATION_RESULT] = ['error']
                    self.log.error(f'{self.file_md5_column} missing at line {line_number}')
                    validation_fail_reason.append(f"{self.file_md5_column}_missing")
                else:
                    if not self.check_md5sum(s3_bucket, s3_file_key, org_obj[self.file_md5_column]):
                        tmp_validation_df[VALIDATION_RESULT] = ['failed']
                        validation_fail_reason.append('file_md5_validation_fail')
                        self.log.error(f"file md5 validation fail at line {line_number}")
            else:
                if error_code == "403":
                    tmp_validation_df[VALIDATION_RESULT] = ['failed']
                    validation_fail_reason.append('forbidden_access_to_s3')
                    self.log.error(f"do not have permission to access the s3 bucket {self.validation_s3_bucket} at line {line_number}")
                else:
                    tmp_validation_df[VALIDATION_RESULT] = ['failed']
                    validation_fail_reason.append('file_not_exist_in_s3')
                    self.log.error(f"file not exist in s3 at line {line_number}")
            if 'passed' in list(tmp_validation_df[VALIDATION_RESULT]):
                #If file validation passed
                self.log.info(f'File validation passed at line {line_number}')
            tmp_validation_df[self.file_name_column] = org_obj[self.file_name_column]
            if self.file_url_column is not None:
                tmp_validation_df[self.file_url_column] = org_obj[self.file_url_column]
            if self.file_size_column is not None:
                tmp_validation_df[self.file_size_column] = org_obj[self.file_size_column]
            tmp_validation_df[self.file_md5_column] = org_obj[self.file_md5_column]
            validation_fail_reason_str = ""
            if len(validation_fail_reason) > 0:
                for i in range(0, len(validation_fail_reason)):
                    if i == 0:
                        validation_fail_reason_str = validation_fail_reason[i]
                    else:
                        validation_fail_reason_str = validation_fail_reason_str + "," + validation_fail_reason[i]
                tmp_validation_df[VALIDATION_FAIL_REASON] = [validation_fail_reason_str]
            validation_df = pd.concat([validation_df, tmp_validation_df])
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        timestamp = get_time_stamp()

        #Start generating validation result file
        validation_file_key = os.path.join(self.output_folder, manifest_file_name.replace(os.path.splitext(manifest_file_name)[1], "_" + timestamp + "-validation-result.tsv"))
        validation_df.to_csv(validation_file_key, sep="\t", index=False)
        zip_file_key = validation_file_key.replace(".tsv", ".zip")
        log_file = get_log_file()
        with zipfile.ZipFile(zip_file_key, 'w') as zipf:
            zipf.write(validation_file_key, os.path.basename(validation_file_key))
            zipf.write(log_file, os.path.basename(log_file))
        #Upload the zip file to s3 bukcet if the manifest csv file is reading from s3 bucket
        if self.download_from_s3:
            try:
                upload_log_file(dest_log_dir, zip_file_key)
                self.log.info(f'Uploading validation result zip file {os.path.basename(zip_file_key)} succeeded!')
            except Exception as e:
                self.log.debug(e)
                self.log.exception(f'File validation failed! Please refer to output file {validation_file_key} and logs {log_file} details')
        if "failed" in list(validation_df[VALIDATION_RESULT]):    
            return False
        else:
            return True

def main(args):
    log = get_logger('Stream File Validator')
    config = process_arguments(args, log)
    stream_file_validator = SteamfileValidator(config.data)
    file_validation_result = stream_file_validator.validate_stream_file()
    if not file_validation_result:
        log.error("File validation failed")
        sys.exit(1)
    else:
        log.info("File validation succeeded")

if __name__ == '__main__':
    main(parse_arguments())
