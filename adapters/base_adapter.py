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

    Following methods are required and subclasses is responsible to provide them, and override above methods if needed
        - get_org_url
    """

    def __init__(self, name_field, md5_field=None):
        self.log = get_logger('Bento_adapter')
        self.name_field = name_field
        self.cleanup_fields = [name_field]
        if md5_field:
            self.md5_field = md5_field
            self.cleanup_fields.append(md5_field)
        else:
            self.md5_field = None
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

    def _get_raw_name(self):
        """
        Get raw value stored in self.name_field, might include path
        :return: file_path
        """
        self._assert_file_info()
        raw_name = self.file_info.get(self.name_field)
        if raw_name is None:
            raise ValueError(f'Can NOT find file name in {self.name_field} field')
        return raw_name

    def get_file_name(self):
        """
        Get file name, without any path
        :return: file name: str
        """
        self._assert_file_info()
        raw_name = self._get_raw_name()

        return os.path.basename(raw_name)

    def get_org_md5(self):
        """
        Get file's original MD5
        :return: MD5: str, None if not available in self.file_info
        """
        self._assert_file_info()
        if self.md5_field:
            return self.file_info.get(self.md5_field)
        else:
            return None

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

