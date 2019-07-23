import logging
import os

LOG_LEVEL = 'DL_LOG_LEVEL'

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
