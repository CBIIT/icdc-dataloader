
from icdc_schema import ICDC_Schema, NODE_TYPE
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
        src = kwargs.get('src')
        node_type = src[NODE_TYPE]
        id_field = self.schema.get_id_field(src)
        node_id = src[id_field]
        if node_type != REGISTRATION_NODE:
            return False

        statement = f'MATCH (c1:case)<--(r:{REGISTRATION_NODE})-->(c2:case) WHERE r.{id_field} = ${id_field} RETURN c1, r, c2'

        # Todo:
        # 1. find all cases of current registration
        #    if more than one cases found:
        #      find all individuals of cases
        #      if there is no individuals:
        #         create new individual with ID based on registration uuid
        #         connect all(both) cases to the new individual
        #      elif there is one individual:
        #          connect all cases to the individual
        #      else (more individuals):
        #          find all cases of all individuals
        #          delete all but the oldest individual
        #          connect all cases including newly found ones to the oldest individual
        #    else: (only one case)
        #        return

        self.log.info(f'Line: {line_num}: {statement}')
