import logging
import os
import re
from urllib.parse import urlparse

from requests import post


def get_logger(name):
    formatter = logging.Formatter('%(asctime)s %(levelname)s: (%(name)s) - %(message)s')
    # formatter = logging.Formatter('[%(levelname)s] %(module)s - %(message)s')
    log_level = os.environ.get('DL_LOG_LEVEL', 'INFO')
    log = logging.getLogger(name)
    log.setLevel(log_level)
    std_handler = logging.StreamHandler()
    std_handler.setFormatter(formatter)
    log.addHandler(std_handler)
    return log


def removeTrailingSlash(uri):
    if uri.endswith('/'):
        return re.sub('/+$', '', uri)
    else:
        return uri

def is_parent_pointer(field_name):
    return re.fullmatch(r'\w+\.\w+', field_name) is not None


def get_host(uri):
    parts = urlparse(uri)
    return parts.hostname

def check_schema_files(schemas, log):
    if not schemas:
        log.error('Please specify schema file(s) with -s or --schema argument')
        return False

    for schema_file in schemas:
        if not os.path.isfile(schema_file):
            log.error('{} is not a file'.format(schema_file))
            return False
    return True


def send_slack_message(url, messaage, log):
    if url:
        headers = {"Content-type": "application/json"}
        result = post(url, json=messaage, headers=headers)
        if not result or result.status_code != 200:
            log.error('Sending Slack messages failed!')
            if result:
                log.error(result.content)
    else:
        log.error('Slack URL not set in configuration file!')


NODES_CREATED = 'nodes_created'
RELATIONSHIP_CREATED = 'relationship_created'
NODES_DELETED = 'nodes_deleted'
RELATIONSHIP_DELETED = 'relationship_deleted'
BLOCK_SIZE = 65536
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y%m%d-%H%M%S'
RELATIONSHIP_TYPE = 'relationship_type'
MULTIPLIER = 'Mul'
DEFAULT_MULTIPLIER = 'many_to_one'
ONE_TO_ONE = 'one_to_one'
UUID = 'uuid'
NEW_MODE = 'new'
UPSERT_MODE = 'upsert'
DELETE_MODE = 'delete'
