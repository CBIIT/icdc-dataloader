import argparse
import os

from config_base import BentoConfig

MASTER_MODE = 'master'
SLAVE_MODE = 'slave'
SOLO_MODE = 'solo'

class Config(BentoConfig):
    valid_modes = [MASTER_MODE, SLAVE_MODE, SOLO_MODE]
    def __init__(self):
        parser = argparse.ArgumentParser(description='Copy files from orginal S3 buckets to specified bucket')
        parser.add_argument('-b', '--bucket', help='Destination bucket name')
        parser.add_argument('--domain', help='Domain name of project')
        parser.add_argument('-p', '--prefix', help='Prefix for files in destination bucket')
        parser.add_argument('-f', '--first', help='First line to load, 1 based not counting headers', type=int)
        parser.add_argument('-c', '--count', help='number of files to copy, -1 means all files in the file', type=int)
        parser.add_argument('--overwrite', help='Overwrite file even same size file already exists at destination',
                            action='store_true')
        parser.add_argument('-d', '--dryrun', help='Only check original file, won\'t copy any files',
                            action='store_true')
        parser.add_argument('-v', '--verify-md5', help='Verify original MD5', action='store_true')
        parser.add_argument('-r', '--retry', help='Number of times to retry', type=int)
        parser.add_argument('-m', '--mode', help='Running mode', choices=self.valid_modes)
        parser.add_argument('--job-queue', help='Job SQS queue name')
        parser.add_argument('--result-queue', help='Result SQS queue name')
        parser.add_argument('--pre-manifest', help='Pre-manifest file')
        parser.add_argument('--adapter-module', help='Adapter module name')
        parser.add_argument('--adapter-class', help='Adapter class name')
        parser.add_argument('config_file', help='Confguration file')
        parser.add_argument('--upload-log-dir', help='Upload destination dir for log file,  if dir in s3, use the format, s3://[bucket]/[prefix]')
        args = parser.parse_args()
        super().__init__(args.config_file, args, 'config_file')

    def validate(self):
        mode = self.data.get('mode')
        if mode is None:
            self.log.critical(f'mode is required, choose from {self.valid_modes}')
            return False
        elif mode not in self.valid_modes:
            self.log.critical(f'mode "{mode}" is not valid, choose from {self.valid_modes}')
            return False

        if mode != SOLO_MODE:
            if not self.data.get('job_queue'):
                self.log.critical(f'job_queue is required in {mode} mode!')
                return False
            if not self.data.get('result_queue'):
                self.log.critical(f'result_queue is required in {mode} mode!')
                return False

        if mode != SLAVE_MODE:
            if not self.data.get('adapter_module'):
                self.log.critical(f'adapter_module is required!')
                return False

            if not self.data.get('adapter_class'):
                self.log.critical(f'adapter_class is required!')
                return False

            if not self.data.get('domain'):
                self.log.critical(f'domain is required in {mode} mode!')
                return False

            if not self.data.get('bucket'):
                self.log.critical(f'bucket is required in {mode} mode!')
                return False

            if not self.data.get('prefix'):
                self.log.critical(f'prefix is required in {mode} mode!')
                return False

            if not self.data.get('pre_manifest'):
                self.log.critical(f'pre_manifest is required in {mode} mode!')
                return False

            if not os.path.isfile(self.data.get('pre_manifest')):
                self.log.critical(f'{self.data.get("pre_manifest")} is not a file!')
                return False

        return True

