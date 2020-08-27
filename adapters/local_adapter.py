#!/usr/bin/env python3
import os
import shutil
import tarfile
from bento.common.utils import removeTrailingSlash
from .base_adapter import BentoAdapter

MEMBER_NAME_INDEX = 0
TAR_NAME_INDEX = 1
TEMP_FOLDER = "tmp"


class BentoLocal(BentoAdapter):
    """
    This adapter assumes all data files are in folder given by parameter data_dir

    Following method is required:
        - get_org_url
    """

    def __init__(self, data_dir, name_field=None, md5_field=None, size_field=None, acl_field=None, location_field=None):
        """

        :param data_dir: location of data files
        :param name_field: field name used to store file name
        :param md5_field: field name used to store original MD5
        :param size_field: field name used to store original file size
        """
        super().__init__(name_field=name_field, md5_field=md5_field, size_field=size_field, acl_field=acl_field,
                         location_field=location_field)
        data_dir = removeTrailingSlash(data_dir)
        if not os.path.isdir(data_dir):
            raise ValueError(f'"{data_dir}" is not a directory!')
        self.data_dir = data_dir
        self.tar_files = {}
        for file in os.listdir(data_dir):
            if file.endswith(".tar"):
                self._scan_files_in_tar(file)

        # Local Adapter specific file info variables
        self.path_in_tar = None

    def load_file_info(self, file_info):
        BentoAdapter.load_file_info(self, file_info)
        if self.get_file_name() in self.tar_files.keys():
            map_data = self.tar_files[self.get_file_name()]
            self.path_in_tar = map_data[MEMBER_NAME_INDEX]
            self._extract_file()

    def clear_file_info(self):
        BentoAdapter.clear_file_info(self)
        self.path_in_tar = None
        tmp_dir = os.path.join(self.data_dir, TEMP_FOLDER)
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)

    def get_org_url(self):
        """
        Get file's URL in original location
        :return: URL: str, will be in file:// scheme if it's local file
        """
        self._assert_file_info()
        return f'file://{self._get_local_path()}'

    def get_org_size(self):
        """
        Get file's original size, if it's not given in file_info, get it from local file
        :return: file_size in bytes
        """
        self._assert_file_info()
        org_size = super().get_org_size()
        if not org_size:
            self.log.info('Original file size is not available, calculate from local file!')
            org_size = os.path.getsize(self._get_local_path())

        return org_size

    def _get_local_path(self):
        if self.path_in_tar:
            return os.path.join(self.data_dir, TEMP_FOLDER, self.path_in_tar)
        else:
            return os.path.join(self.data_dir, self._get_path())

    def _scan_files_in_tar(self, tar_name):
        tar = tarfile.open(os.path.join(self.data_dir, tar_name))
        for member in tar.getmembers():
            if not member.isdir():
                file_name = os.path.basename(member.name)
                self.tar_files[file_name] = (member.name, tar_name)

    def _extract_file(self):
        """
        Extracts the file from the tar into "<data dir>/tmp/"
        """
        self._assert_file_info()
        if self.get_file_name() in self.tar_files.keys():
            tar_name = self.tar_files[self.get_file_name()][TAR_NAME_INDEX]
            member_name = self.tar_files[self.get_file_name()][MEMBER_NAME_INDEX]
            tar = tarfile.open(os.path.join(self.data_dir, tar_name))
            temp_dir = os.path.join(self.data_dir, TEMP_FOLDER)
            tar.extract(member_name, temp_dir)
            self.path_in_tar = member_name
