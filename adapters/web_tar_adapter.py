import os
import requests
from adapters.local_adapter import BentoLocal


class BentoWebTar(BentoLocal):
    """
    This adapter downloads the tar at the given url to be used with the local adapter
    """

    def __init__(self, tar_url, data_dir, name_field=None, md5_field=None, size_field=None, acl_field=None,
                 location_field=None):
        request = requests.get(tar_url, allow_redirects=True)
        self.temp_tar_path = os.path.join(data_dir, 'temp.tar')
        open(self.temp_tar_path, mode='wb').write(request.content)
        super().__init__(data_dir, name_field=name_field, md5_field=md5_field, size_field=size_field, acl_field=acl_field,
                         location_field=location_field)

    def clear_temp_tar(self):
        if os.path.exists(self.temp_tar_path):
            os.remove(str(self.temp_tar_path))


