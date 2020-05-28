#!/usr/bin/env python3
import requests

from bento.common.utils import removeTrailingSlash, get_logger


class Glioma:
    url_prefix = 'https://sra-pub-src-1.s3.amazonaws.com'
    bucket_name = 'sra-pub-src-1'
    cleanup_fields = ['original_md5', 'SRA_accession']
    max_version = 10


    def __init__(self, prefix):
        if prefix and isinstance(prefix, str):
            self.prefix = removeTrailingSlash(prefix)
        else:
            raise ValueError(f'Invalid prefix: "{prefix}"')

        self.log = get_logger('Glioma_adapter')

    def _assert_file_info(self):
        if not hasattr(self, 'file_info') or not self.file_info:
            raise Exception('file_info is empty, call load_file_info() method first!')

    @staticmethod
    def _dash_to_underscore(input):
        return input.replace('-', '_')

    def load_file_info(self, file_info):
        self.file_info = file_info

    def clear_file_info(self):
        self.file_info = {}

    def get_org_url(self):
        self._assert_file_info()
        real_name = self._dash_to_underscore(self.file_info.get('file_name'))
        for i in range(1, self.max_version):
            url = 'https://{}.s3.amazonaws.com/{}/{}.{}'.format(self.bucket_name,
                                                                self.file_info.get('SRA_accession'),
                                                                real_name,
                                                                i)
            r = requests.head(url)
            if r.ok:
                return url
            else:
                self.log.info(f'File {real_name} version {i} doesn\'t exist')

        raise LookupError(f'Couldn\'t find file {real_name}!')
        # return 'https://sra-pub-src-1.s3.amazonaws.com/SRR10386332/CGP_S03_5E9A_305F1E05_T1_A1_J05.bam.1'

    def get_dest_key(self):
        self._assert_file_info()
        name = self.file_info.get('file_name')
        return f'{self.prefix}/{name}'

    def get_org_md5(self):
        self._assert_file_info()
        return self.file_info.get('original_md5')

    def get_file_name(self):
        self._assert_file_info()
        return self.file_info.get('file_name')

    def get_fields(self):
        obj = {}
        for key, val in self.file_info.items():
            if key in self.cleanup_fields:
                continue
            else:
                obj[key] = val

        return obj

    def filter_fields(self, fields):
        return list(filter(lambda f: f not in self.cleanup_fields, fields))


