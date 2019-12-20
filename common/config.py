from configparser import ConfigParser
import os, sys

import yaml

from .utils import get_logger

PSWD_ENV = 'NEO_PASSWORD'
util_log = get_logger('Utils')
config = ConfigParser()
CONFIG_FILE_ENV_VAR = 'ICDC_DATA_LOADER_CONFIG'
config_file = os.environ.get(CONFIG_FILE_ENV_VAR, 'config/config.ini')
if config_file and os.path.isfile(config_file):
    config.read(config_file)
else:
    util_log.error('Can\'t find configuration file! Make a copy of config.sample.ini to config.ini'
                   + ' or specify config file in Environment variable {}'.format(CONFIG_FILE_ENV_VAR))
    sys.exit(1)

LOG_LEVEL = os.environ.get('DL_LOG_LEVEL', config.get('log', 'log_level'))
ICDC_DOMAIN = config.get('main', 'domain')
QUEUE_LONG_PULL_TIME = int(config.get('sqs', 'long_pull_time'))
VISIBILITY_TIMEOUT = int(config.get('sqs', 'visibility_timeout'))

TEMP_FOLDER = config.get('main', 'temp_folder')
BACKUP_FOLDER = config.get('main', 'backup_folder')
INDEXD_GUID_PREFIX = config.get('indexd', 'GUID_prefix')
INDEXD_MANIFEST_EXT = config.get('indexd', 'ext')

if not INDEXD_MANIFEST_EXT.startswith('.'):
    INDEXD_MANIFEST_EXT = '.' + INDEXD_MANIFEST_EXT
os.makedirs(BACKUP_FOLDER, exist_ok=True)
if not os.path.isdir(BACKUP_FOLDER):
    util_log.error('{} is not a folder!'.format(BACKUP_FOLDER))
    sys.exit(1)

SLACK_URL = config.get('slack', 'url')

PROP_FILE_ENV_VAR = 'ICDC_DATA_LOADER_PROP'
property_file = os.environ.get(PROP_FILE_ENV_VAR, 'config/props.yml')
if property_file and os.path.isfile(property_file):
    with open(property_file) as prop_file:
        PROPS = yaml.safe_load(prop_file)['Properties']
        if not PROPS:
            util_log.error('Can\'t read property file!')
            sys.exit(1)
else:
    util_log.error(
        'Can\'t find property file! Get a copy of prop.yml or specify property file in Environment variable {}'.format(
            PROP_FILE_ENV_VAR))
    sys.exit(1)


