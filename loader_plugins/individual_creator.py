from icdc_schema import ICDC_Schema, NODE_TYPE
from bento.common.utils import get_logger, NODE_LOADED
from data_loader import CREATED
from bento.common.utils import UUID

REGISTRATION_NODE = 'registration'
CASE_NODE = 'case'
INDIVIDUAL_NODE = 'canine_individual'

class IndividualCreator:
    def __init__(self, schema):
        if not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        self.schema = schema
        self.log = get_logger('VisitCreator')
        self.nodes_created = 0
        self.nodes_updated = 0
        self.relationships_created = 0
        self.nodes_stat = {}
        self.nodes_stat_updated = {}
        self.relationships_stat = {}

    # Will be called to determine if plugin needs to be run for node_type and event
    def should_run(self, node_type, event):
        return node_type == REGISTRATION_NODE and event == NODE_LOADED

    # Create individual node if needed (a registration node connects more than one case node)
    def create_node(self, session, **kwargs):
        individual_created = False
        line_num = kwargs.get('line_num')
        src = kwargs.get('src')
        node_type = src[NODE_TYPE]
        id_field = self.schema.get_id_field(src)
        node_id = src[id_field]
        if node_type != REGISTRATION_NODE:
            return False

        statement = f'''
            MATCH (c:{CASE_NODE})<--(r:{REGISTRATION_NODE}) 
            OPTIONAL match (c)-->(i:{INDIVIDUAL_NODE})
            WITH r, collect(DISTINCT c) AS cc, collect(distinct i) AS ci 
            WHERE size(cc) > 1 and r.{id_field} = ${id_field}
            RETURN cc, ci
        '''
        result = session.run(statement, {id_field: node_id})
        for r in result:
            individual_nodes = r.get('ci')
            case_nodes = r.get('cc')
            if len(individual_nodes) > 1:
                # Todo:
                #      if found more individuals:
                #          find all cases of all individuals
                #          delete all but the oldest individual
                #          connect all cases including newly found ones to the oldest individual
                msg = f"Line: {line_num}: More than one individuals associated with one dog!"
                self.log.error(msg)
                raise Exception(msg)
            elif len(individual_nodes) == 1:
                individual = individual_nodes[0]
                i_id = individual.id
            elif len(individual_nodes) == 0:
                individual_id = self.schema.get_uuid_for_node(INDIVIDUAL_NODE, node_id)
                i_id = self.create_individual(session, individual_id)
                individual_created = True

            for case in case_nodes:
                self.connect_case_to_individual(session, case.id, i_id)

            return individual_created

    def create_individual(self, session, uuid):
        id_field = self.schema.props.id_fields.get(INDIVIDUAL_NODE)
        statement = f'''
            MATCH (i:{INDIVIDUAL_NODE}) WITH apoc.number.format(coalesce(max(toInteger(i.canine_individual_id)) + 1, 
            1), '0000') AS i_id CREATE (i:{INDIVIDUAL_NODE} {{ {id_field}: i_id, {CREATED}: datetime(), 
            {UUID}:${UUID} }}) 
            RETURN id(i) AS node_id
            '''
        result = session.run(statement, {UUID: uuid})
        if result:
            i_id = result.single()
            count = result.consume().counters.nodes_created
            self.nodes_created += count
            # count the updated nodes
            update_count = 0
            if result.consume().counters.nodes_created == 0 and result.consume().counters._contains_updates:
                update_count = 1
            self.nodes_updated += update_count
            self.nodes_stat[INDIVIDUAL_NODE] = self.nodes_stat.get(INDIVIDUAL_NODE, 0) + count
            self.nodes_stat_updated[INDIVIDUAL_NODE] = self.nodes_stat_updated.get(INDIVIDUAL_NODE, 0) + update_count
            return i_id[0]
        else:
            return None

    def connect_case_to_individual(self, session, c_id, i_id):
        relationship_name = self.schema.get_relationship(CASE_NODE, INDIVIDUAL_NODE).get('relationship_type')
        statement = f'''
            MATCH (i:{INDIVIDUAL_NODE})
            WHERE id(i) = $i_id
            MATCH (c:{CASE_NODE})
            WHERE id(c) = $c_id
            MERGE (c)-[r:{relationship_name}]->(i)
              ON CREATE SET r.{CREATED} = datetime()
            '''
        result = session.run(statement, {'c_id': c_id, 'i_id': i_id})
        if result:
            count = result.consume().counters.relationships_created
            self.relationships_created += count
            self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + count
