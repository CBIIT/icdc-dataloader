#!/usr/bin/env python3
import requests

from .web_adapter import BentoWeb


class Glioma(BentoWeb):
    """
    Following method is required:
        - get_org_url
    """
    max_version = 10


    def __init__(self):
        """

        :param working_dir: location of pre-manifest and files, all adapters should accept this parameter!
        """
        super().__init__(name_field='file_name', md5_field='original_md5', url_prefix='https://sra-pub-src-1.s3.amazonaws.com')
        self.cleanup_fields.append('SRA_accession')

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

