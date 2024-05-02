import os
import yaml
from bento.common.utils import get_logger

class Props:
    def __init__(self, file_name):
        self.log = get_logger('Props')
        if file_name and os.path.isfile(file_name):
            with open(file_name) as prop_file:
                props = yaml.safe_load(prop_file)['Properties']
                if not props:
                    msg = 'Can\'t read property file!'
                    self.log.error(msg)
                    raise Exception(msg)
                self.plurals = props.get('plurals', {})
                self.type_mapping = props.get('type_mapping', {})
                self.id_fields = props.get('id_fields', {})
                self.visit_date_in_nodes = props.get('visit_date_in_nodes', {})
                self.domain = props.get('domain', 'Unknown.domain.nci.nih.gov')
                self.rel_prop_delimiter = props.get('rel_prop_delimiter', '$')
                self.indexes = props.get('indexes', [])
                self.save_parent_id = props.get('save_parent_id', [])
                self.delimiter = props.get("delimiter", "|")
        else:
            msg = f'Can NOT open file: "{file_name}"'
            self.log.error(msg)
            raise Exception(msg)
