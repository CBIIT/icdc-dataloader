import logging
import os
import uuid

LOG_LEVEL = 'DL_LOG_LEVEL'
ICDC_DOMAIN = 'caninecommons.cancer.gov'
QUEUE_LONG_PULL_TIME = 20
VISIBILITY_TIMEOUT = 30


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

def get_uuid_for_node(node_type, signature):
    """Generate V5 UUID for a node
    Arguments:
        node_type - a string represents type of a node, e.g. case, study, file etc.
        signature - a string that can uniquely identify a node within it's type, e.g. case_id, clinical_study_designation etc.
                    or a long string with all properties and values concat together if no id available

    """
    log = get_logger('Utils')
    icdc_base_uuid = uuid.uuid5(uuid.NAMESPACE_URL, ICDC_DOMAIN)
    # log.debug('Base UUID: {}'.format(icdc_base_uuid))
    type_uuid = uuid.uuid5(icdc_base_uuid, node_type)
    # log.debug('Type UUID: {}'.format(type_uuid))
    node_uuid = uuid.uuid5(type_uuid, signature)
    log.debug('Node UUID: {}'.format(node_uuid))
    return node_uuid
