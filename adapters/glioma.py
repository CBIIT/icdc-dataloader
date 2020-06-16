#!/usr/bin/env python3
import requests

from bento.common.utils import get_logger


class Glioma:
    """
    Following methods are required:
        - filter_fields
        - get_fields
        - load_file_info
        - clear_file_info
        - get_org_url
        - get_file_name
        - get_org_md5
    """
    url_prefix = 'https://sra-pub-src-1.s3.amazonaws.com'
    cleanup_fields = ['original_md5', 'SRA_accession']
    max_version = 10


    def __init__(self):
        self.log = get_logger('Glioma_adapter')

    def _assert_file_info(self):
        if not hasattr(self, 'file_info') or not self.file_info:
            raise Exception('file_info is empty, call load_file_info() method first!')

    @staticmethod
    def _dash_to_underscore(input):
        return input.replace('-', '_')

    def load_file_info(self, file_info):
        """
        Load new file information
        :param file_info:
        :return: None
        """
        self.file_info = file_info

    def clear_file_info(self):
        """
        Clear last file information loaded
        :return: None
        """
        self.file_info = {}

    def get_org_url(self):
        """
        Get file's URL in original location
        :return: URL: str, will be in file:// scheme if it's local file
        """
        self._assert_file_info()
        real_name = self._dash_to_underscore(self.file_info.get('file_name'))
        for i in range(1, self.max_version):
            url = '{}/{}/{}.{}'.format(self.url_prefix, self.file_info.get('SRA_accession'), real_name, i)
            r = requests.head(url)
            if r.ok:
                return url
            else:
                self.log.info(f'File {real_name} version {i} doesn\'t exist')

        raise LookupError(f'Couldn\'t find file {real_name}!')
        # return 'https://sra-pub-src-1.s3.amazonaws.com/SRR10386332/CGP_S03_5E9A_305F1E05_T1_A1_J05.bam.1'

    def get_org_md5(self):
        """
        Get file's original MD5
        :return: MD5: str, None if not available in self.file_info
        """
        self._assert_file_info()
        return self.file_info.get('original_md5')

    def get_file_name(self):
        """
        Get file name, without any path
        :return: file name: str
        """
        self._assert_file_info()
        return self.file_info.get('file_name')

    def get_fields(self):
        """
        Get available fields exclude the ones in self.cleanup_fields in self.file_info
        :return:
        """
        obj = {}
        for key, val in self.file_info.items():
            if key in self.cleanup_fields:
                continue
            else:
                obj[key] = val

        return obj

    def filter_fields(self, fields):
        """
        Remove all fields that's in self.cleanup_fields from input field list
        :param fields: list
        :return: field list with unwanted fields removed
        """
        return list(filter(lambda f: f not in self.cleanup_fields, fields))


