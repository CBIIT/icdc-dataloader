#!/usr/bin/env python3
import os

from bento.common.utils import get_logger, get_md5, removeTrailingSlash
from .base_adapter import BentoAdapter


class BentoLocal(BentoAdapter):
    """
    This adapter assumes all data files are in folder given by parameter data_dir

    Following methods are required:
        - get_org_url
        - get_org_md5
    """

    def __init__(self, data_dir, name_field='file_name', md5_field=None, size_field=None, verify=True):
        """

        :param data_dir: location of data files
        :param name_field: field name used to store file name
        :param md5_field: field name used to store original MD5
        :param size_field: field name used to store original file size
        :param verify: whether or not to verify MD5 and size
        """
        super().__init__(name_field, md5_field)
        data_dir = removeTrailingSlash(data_dir)
        if not os.path.isdir(data_dir):
            raise ValueError(f'"{data_dir}" is not a directory!')
        self.data_dir = data_dir
        self.size_field = size_field
        if size_field is not None:
            self.cleanup_fields.append(size_field)
        self.verify = verify

    def get_org_url(self):
        """
        Get file's URL in original location
        :return: URL: str, will be in file:// scheme if it's local file
        """
        self._assert_file_info()
        return f'file://{self._get_local_path()}'

    def _get_local_path(self):
        return f'{self.data_dir}/{self._get_raw_name()}'

    def get_org_md5(self):
        """
        Get file's original MD5,
        If original file's MD5 is given in column named in self.md5_field, then it will be used to verify file content
        If original file's size is given in column named in self.size_field, then file size will be verified also
        :return: MD5: str, None if not available in self.file_info
        """
        self._assert_file_info()
        org_md5 = super().get_org_md5()
        if not org_md5 or self.verify:
            real_md5 = get_md5(self._get_local_path())

            if not org_md5:
                return real_md5
            else: # Must verify
                if org_md5 != real_md5:
                    self.log.error(f'File content does NOT match given MD5: {self._get_raw_name()}!')
                    raise ValueError(f'MD5 verification failed!')
        if not self._check_file_size():
            self.log.error(f'File size does NOT match given size: {self._get_raw_name()}!')
            raise ValueError(f'File size verification failed!')

        return org_md5

    def _check_file_size(self):
        """
        Verify local file size
        :return: bool
        """
        if self.size_field and self.verify:
            org_size = int(self.file_info.get(self.size_field))
            real_size = os.path.getsize(self._get_local_path())
            return org_size == real_size
        else:
            return True



