#!/usr/bin/env python3
import os
import requests

from bento.common.utils import get_logger
from .base_adapter import BentoAdapter


class Glioma(BentoAdapter):
    """
    Following methods are required:
        - get_org_url
        - get_org_md5
    """
    url_prefix = 'https://sra-pub-src-1.s3.amazonaws.com'
    cleanup_fields = ['original_md5', 'SRA_accession']
    max_version = 10


    def __init__(self):
        """

        :param working_dir: location of pre-manifest and files, all adapters should accept this parameter!
        """
        super().__init__(name_field='file_name', md5_field='original_md5')

    @staticmethod
    def _dash_to_underscore(input):
        return input.replace('-', '_')

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

