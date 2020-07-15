import os

from bento.common.utils import get_logger


class BentoAdapter:
    """
    Base adapter class provides common methods

    Following methods are required for an adapter and are provided in this class
        - filter_fields
        - get_fields
        - load_file_info
        - clear_file_info
        - get_file_name
        - get_org_md5
        - get_org_size

    Following methods are required and subclasses is responsible to provide them, and override above methods if needed
        - get_org_url
    """
    DEFAULT_ACL = "['Open']"

    def __init__(self, name_field=None, md5_field=None, size_field=None, acl_field=None, location_field=None):
        """

        :param name_field: field name that contains file names, or locations of files if not using location_field parameter
        :param md5_field: field name that contains original MD5
        :param size_field: field name that contains original file size
        :param acl_field: field name that contains original ACL
        :param location_field: field name that contains file location, if this parameter is used, name_field should
                               only contains file names
        """
        self.log = get_logger('Bento_adapter')
        self.location_field = location_field if location_field else 'file_location'
        self.name_field = name_field if name_field else 'file_name'
        self.md5_field = md5_field if md5_field else 'md5sum'
        self.acl_field = acl_field if acl_field else 'acl'
        self.size_field = size_field if size_field else 'file_size'

        self.cleanup_fields = [self.name_field, self.md5_field, self.size_field, self.acl_field, self.location_field]
        self.file_info = {}

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

    def _assert_file_info(self):
        if not hasattr(self, 'file_info') or not self.file_info:
            raise Exception('file_info is empty, call load_file_info() method first!')

    def _get_path(self):
        """
        Get file path from location_field if available, otherwise get from name_field
        :return: file_path
        """
        self._assert_file_info()
        path = self.file_info.get(self.location_field)
        if not path:
            path = self.file_info.get(self.name_field)

        if not path:
            raise ValueError(f'Can NOT find file path!')
        return path

    def get_file_name(self):
        """
        Get file name, without any path
        :return: file name: str
        """
        self._assert_file_info()
        raw_name = self.file_info.get(self.name_field)
        if not raw_name:
            raise ValueError(f'Can NOT find file name!')

        return os.path.basename(raw_name)

    def get_org_md5(self):
        """
        Get file's original MD5
        :return: MD5: str, None if not available in self.file_info
        """
        self._assert_file_info()
        return self.file_info.get(self.md5_field)

    def get_org_size(self):
        """
        Get file's original size
        :return: size in bytes
        """
        self._assert_file_info()
        try:
            raw_size = self.file_info.get(self.size_field)
            return float(raw_size) if raw_size else None
        except Exception as e:
            self.log.error('Failed to get file size!')
            self.log.exception(e)
            return None

    def get_acl(self):
        """
        Get file's ACL if given, other wise return DEFAULT_ACL
        :return: str
        """
        self._assert_file_info()
        acl = self.file_info.get(self.acl_field)
        return acl if acl else self.DEFAULT_ACL

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

