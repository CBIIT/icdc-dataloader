import os

import yaml

from bento.common.utils import get_logger


class BentoConfig:
    def __init__(self, config_file, args, config_file_arg='config_file'):
        self.log = get_logger('Bento Config')
        if not config_file:
            raise ValueError(f'Empty config file name')
        if not os.path.isfile(config_file):
            raise ValueError(f'"{config_file}" is not a file!')

        self.config_file_arg = config_file_arg

        with open(config_file) as c_file:
            self.data = yaml.safe_load(c_file)['Config']
            if self.data is None:
                self.data = {}

        self._override(args)

    def _override(self, args):
        for key, value in vars(args).items():
            # Ignore config file argument
            if key == self.config_file_arg:
                continue
            if isinstance(value, bool):
                if value:
                    self.data[key] = value

            elif value is not None:
                self.data[key] = value

    def create_folder(self, folder):
        """
        Create given folder if not already exists
        :param folder: folder path
        :return:
        """
        os.makedirs(folder, exist_ok=True)
        if not os.path.isdir(folder):
            msg = f'{folder} is not a folder!'
            self.log.error(msg)
            raise Exception(msg)

