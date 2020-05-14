#!/usr/bin/env python3

from bento.common.utils import removeTrailingSlash
class Glioma:
    url_prefix = 'https://sra-pub-src-1.s3.amazonaws.com'
    bucket_name = 'sra-pub-src-1'
    def __init__(self, prefix):
        if prefix and isinstance(prefix, str):
            self.prefix = removeTrailingSlash(prefix)
        else:
            raise ValueError(f'Invalid prefix: "{prefix}"')

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
        return 'https://{}.s3.amazonaws.com/{}/{}.1'.format(self.bucket_name, self.file_info.get('SRA_accession'), real_name)
        # return 'https://sra-pub-src-1.s3.amazonaws.com/SRR10386332/CGP_S03_5E9A_305F1E05_T1_A1_J05.bam.1'

    def get_dest_key(self):
        self._assert_file_info()
        name = self.file_info.get('file_name')
        return f'{self.prefix}/{name}'

    def get_org_md5(self):
        self._assert_file_info()
        return self.file_info.get('original_md5')

