#!/usr/bin/env python3
import os

from bento.common.utils import get_logger, removeTrailingSlash
from .base_adapter import BentoAdapter


class BentoWeb(BentoAdapter):
    """
    This adapter handles original files are publicly accessible on the web
    Pre-manifest file can contain Original URLs or file names and a common URL prefix given in a parameter

    Following methods are required:
        - get_org_url
        - get_org_md5
    """


    def __init__(self, name_field='file_name', md5_field=None, size_field=None, url_prefix=None, verify=True):
        """
        If url_prefix is given, then it will prepend to file names to get original URL,
        Otherwise, it will assume name_field contains complete URLs

        :param name_field: field name used to store file name
        :param md5_field: field name used to store original MD5
        :param size_field: field name used to store original file size
        :param url_prefix: URL prefix to prepend to all file names
        :param verify: whether or not to verify MD5 and size
        """
        super().__init__(name_field, md5_field)
        if isinstance(url_prefix, str) and url_prefix:
            self.url_prefix = removeTrailingSlash(url_prefix)
        else:
            self.url_prefix = None

        self.cleanup_fields = [name_field]

        if isinstance(size_field, str) and size_field:
            self.size_field = size_field
            self.cleanup_fields.append(size_field)
        else:
            self.size_field = None

        self.verify = verify

    def get_org_url(self):
        """
        Get file's URL in original location
        :return: URL: str, will be in file:// scheme if it's local file
        """
        if self.url_prefix:
            return f'{self.url_prefix}/{self._get_raw_name()}'
        else:
            return self.file_info.get(self.name_field)

