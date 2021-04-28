
from icdc_schema import ICDC_Schema
from bento.common.utils import get_logger, NODE_LOADED

REGISTRATION_NODE = 'registration'

class IndividualCreator:
    def __init__(self, schema):
        if not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        self.schema = schema
        self.log = get_logger('VisitCreator')
        self.nodes_created = 0
        self.relationships_created = 0
        self.nodes_stat = {}
        self.relationships_stat = {}

    # Will be called to determine if plugin needs to be run for node_type and event
    def should_run(self, node_type, event):
        return node_type == REGISTRATION_NODE and event == NODE_LOADED

    def create_node(self, session, **kwargs):
        line_num = kwargs.get('line_num')
        node_type = kwargs.get('node_type')
        node_id = kwargs.get('node_id')
        src = kwargs.get('src')

        self.log.info(f'Line: {line_num} (:{node_type} {node_id}) src: {src}')
