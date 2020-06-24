#!/usr/bin/env python3
import os

from bento.common.utils import removeTrailingSlash
from .base_adapter import BentoAdapter


class BentoLocal(BentoAdapter):
    """
    This adapter assumes all data files are in folder given by parameter data_dir

    Following method is required:
        - get_org_url
    """

    def __init__(self, data_dir, name_field=None, md5_field=None, size_field=None, acl_field=None):
        """

        :param data_dir: location of data files
        :param name_field: field name used to store file name
        :param md5_field: field name used to store original MD5
        :param size_field: field name used to store original file size
        """
        super().__init__(name_field=name_field, md5_field=md5_field, size_field=size_field, acl_field=acl_field)
        data_dir = removeTrailingSlash(data_dir)
        if not os.path.isdir(data_dir):
            raise ValueError(f'"{data_dir}" is not a directory!')
        self.data_dir = data_dir

    def get_org_url(self):
        """
        Get file's URL in original location
        :return: URL: str, will be in file:// scheme if it's local file
        """
        self._assert_file_info()
        return f'file://{self._get_local_path()}'

    def _get_local_path(self):
        return os.path.join(self.data_dir, self._get_raw_name())

    def get_org_size(self):
        """
        Get file's original size, if it's not given in file_info, get it from local file
        :return: file_size in bytes
        """
        self._assert_file_info()
        org_size = super().get_org_size()
        if not org_size:
            org_size = os.path.getsize(self._get_local_path())

        return org_size

