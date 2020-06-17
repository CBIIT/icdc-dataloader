#!/usr/bin/env python3
import os

from bento.common.utils import get_logger, get_md5


class BentoLocal:
    """
    This adapter assumes all data files are in same folder as the pre-manifest
    Following methods are required:
        - filter_fields
        - get_fields
        - load_file_info
        - clear_file_info
        - get_org_url
        - get_file_name
        - get_org_md5
    """
    # Following column must exist in pre-manifest file
    name_field = 'file_name'
    # Following column is optional in pre-manifest file, if omit MD5 will be computed from local file
    md5_field = 'md5sum'

    cleanup_fields = [md5_field, name_field]

    def __init__(self, working_dir):
        self.log = get_logger('Local_adapter')
        if not os.path.isdir(working_dir):
            raise ValueError(f'"{working_dir}" is not a directory!')
        self.working_dir = working_dir

    def _assert_file_info(self):
        if not hasattr(self, 'file_info') or not self.file_info:
            raise Exception('file_info is empty, call load_file_info() method first!')

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
        return f'file://{self._get_local_path()}'

    def _get_local_path(self):
        return f'{self.working_dir}/{self.get_file_name()}'

    def get_org_md5(self):
        """
        Get file's original MD5
        :return: MD5: str, None if not available in self.file_info
        """
        self._assert_file_info()
        org_md5 = self.file_info.get(self.md5_field)
        if not org_md5:
            org_md5 = get_md5(self._get_local_path())

        return org_md5

    def get_file_name(self):
        """
        Get file name, without any path
        :return: file name: str
        """
        self._assert_file_info()
        file_name = self.file_info.get(self.name_field)
        if file_name is None:
            raise ValueError(f'Can NOT find file name in {self.name_field} field')
        return file_name

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


