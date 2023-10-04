from datetime import timedelta

from neo4j import Session, Transaction

from icdc_schema import ICDC_Schema
from bento.common.utils import get_logger, UUID, RELATIONSHIP_TYPE, MISSING_PARENT, parse_date, \
    date_to_string

VISIT_NODE = 'visit'
VISIT_ID = 'visit_id'
VISIT_DATE = 'visit_date'
OF_CYCLE = 'of_cycle'
CYCLE_NODE = 'cycle'
INFERRED = 'inferred'
START_DATE = 'date_of_cycle_start'
END_DATE = 'date_of_cycle_end'
CYCLE_ID = 'cycle_id'

PREDATE = 7
FOREVER = '9999-12-31'

# duplicated declaration from data_loader.py
NODE_TYPE = 'type'
CREATED = 'created'
UPDATED = 'updated'
CASE_ID = 'case_id'
CASE_NODE = 'case'


# Intermediate node creator
class VisitCreator:
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
        # Dictionary to cache case IDs and their associated cycles in order to prevent redundant querying
        self.cycle_map = {}

    def should_run(self, node_type, event):
        return node_type == VISIT_NODE and event == MISSING_PARENT

    def create_node(self, session, line_num, node_type, node_id, src):
        if node_type != VISIT_NODE:
            self.log.debug("Line: {}: Won't create node for type: '{}'".format(line_num, VISIT_NODE))
            return False
        if not node_id:
            self.log.error("Line: {}: Can't create (:{}) node for id: '{}'".format(line_num, VISIT_NODE, node_id))
            return False
        if not src:
            self.log.error("Line: {}: Can't create (:{}) node for empty object".format(line_num, VISIT_NODE))
            return False
        if not session or (not isinstance(session, Session) and not isinstance(session, Transaction)):
            self.log.error("Neo4j session is not valid!")
            return False
        date_map = self.schema.props.visit_date_in_nodes
        if NODE_TYPE not in src:
            self.log.error('Line: {}: Given object doesn\'t have a "{}" field!'.format(line_num, NODE_TYPE))
            return False
        source_type = src[NODE_TYPE]
        date = src[date_map[source_type]]
        if not date:
            self.log.error('Line: {}: Visit date is empty!'.format(line_num))
            return False
        if NODE_TYPE not in src:
            self.log.error('Line: {}: Given object doesn\'t have a "{}" field!'.format(line_num, NODE_TYPE))
            return False
        statement = 'MERGE (v:{} {{ {}: $node_id, {}: $date, {}: true, {}: ${} }})'.format(
            VISIT_NODE, VISIT_ID, VISIT_DATE, INFERRED, UUID, UUID)
        statement += ' ON CREATE SET v.{} = datetime()'.format(CREATED)
        statement += ' ON MATCH SET v.{} = datetime()'.format(UPDATED)

        result = session.run(statement, {"node_id": node_id, "date": date,
                                         UUID: self.schema.get_uuid_for_node(VISIT_NODE, node_id)})
        if result:
            count = result.consume().counters.nodes_created
            self.nodes_created += count
            #count the updated nodes
            update_count = 0
            if result.consume().counters.nodes_created == 0 and result.consume().counters._contains_updates:
                update_count = 1
            self.nodes_updated += update_count
            self.nodes_stat[VISIT_NODE] = self.nodes_stat.get(VISIT_NODE, 0) + count
            self.nodes_stat_updated[VISIT_NODE] = self.nodes_stat_updated.get(VISIT_NODE, 0) + update_count
            if count > 0:
                case_id = src[CASE_ID]
                if not self.connect_visit_to_cycle(session, line_num, node_id, case_id, date):
                    self.log.error('Line: {}: Visit: "{}" does NOT belong to a cycle!'.format(line_num, node_id))
                return True
        else:
            return False

    def connect_visit_to_cycle(self, session, line_num, visit_id, case_id, visit_date):
        cycle_data_array = []
        if case_id not in self.cycle_map:
            find_cycles_stmt = 'MATCH (c:cycle) WHERE c.case_id = $case_id RETURN c ORDER BY c.date_of_cycle_start'
            result = session.run(find_cycles_stmt, {'case_id': case_id})
            if result:
                # Iterates through each record in the result
                for record in result:
                    # Retrieves the cycle object from the record
                    cycle = record.data()['c']
                    # Stores the relevant cycle data in a dictionary
                    formatted_start_date = parse_date(cycle[START_DATE])
                    try:
                        formatted_end_date = parse_date(cycle[END_DATE])
                    except ValueError:
                        formatted_end_date = None
                    cycle_data = {
                        START_DATE: formatted_start_date,
                        END_DATE: formatted_end_date,
                        CYCLE_ID: record[0].id
                    }
                    # Adds the dictionary to an array for storage
                    cycle_data_array.append(cycle_data)
                # The array of cycle data dictionaries is added to the cycle map
                self.cycle_map[case_id] = cycle_data_array
        else:
            cycle_data_array = self.cycle_map[case_id]
        if len(cycle_data_array) > 0:
            first_date = None
            pre_date = None
            relationship_name = self.schema.get_relationship(VISIT_NODE, CYCLE_NODE)[RELATIONSHIP_TYPE]
            if not relationship_name:
                return False
            for cycle_data in cycle_data_array:
                date = parse_date(visit_date)
                start_date = cycle_data[START_DATE]
                if not first_date:
                    first_date = start_date
                    pre_date = first_date - timedelta(days=PREDATE)
                if cycle_data[END_DATE]:
                    end_date = cycle_data[END_DATE]
                else:
                    self.log.warning('Line: {}: No end dates for cycle started on {} for {}'
                                     .format(line_num, date_to_string(start_date), case_id))
                    end_date = parse_date(FOREVER)
                if (start_date <= date <= end_date) or (first_date > date >= pre_date):
                    if first_date > date >= pre_date:
                        self.log.info(
                            'Line: {}: Date: {} is before first cycle, but within {}'.format(line_num, visit_date,
                                                                                             PREDATE)
                            + ' days before first cycle started: {}, connected to first cycle'
                            .format(date_to_string(first_date)))
                    connect_stmt = 'MATCH (v:{} {{ {}: $visit_id }}) '.format(VISIT_NODE, VISIT_ID)
                    connect_stmt += 'MATCH (c:{}) WHERE id(c) = $cycle_id '.format(CYCLE_NODE)
                    connect_stmt += 'MERGE (v)-[r:{} {{ {}: true }}]->(c)'.format(relationship_name, INFERRED)
                    connect_stmt += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
                    connect_stmt += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)

                    cnt_result = session.run(connect_stmt, {'visit_id': visit_id, 'cycle_id': cycle_data[CYCLE_ID]})
                    relationship_created = cnt_result.consume().counters.relationships_created
                    if relationship_created > 0:
                        self.relationships_created += relationship_created
                        self.relationships_stat[relationship_name] = \
                            self.relationships_stat.get(relationship_name, 0) + relationship_created
                        return True
                    else:
                        self.log.error(
                            'Line: {}: Create (:visit)-[:of_cycle]->(:cycle) relationship failed!'.format(line_num))
                        return False
            self.log.warning('Line: {}: Date: {} does not belong to any cycles, connected to case {} directly!'.format(
                line_num, visit_date, case_id))
            return self.connect_visit_to_case(session, line_num, visit_id, case_id)
        else:
            self.log.error('Line: {}: No cycles found for case: {}'.format(line_num, case_id))
            return False

    def connect_visit_to_case(self, session, line_num, visit_id, case_id):
        relationship_name = self.schema.get_relationship(VISIT_NODE, CASE_NODE)[RELATIONSHIP_TYPE]
        if not relationship_name:
            return False
        cnt_statement = 'MATCH (c:case {{ case_id: $case_id }}) MATCH (v:visit {{ {}: $visit_id }}) '.format(
            VISIT_ID)
        cnt_statement += 'MERGE (c)<-[r:{} {{ {}: true }}]-(v)'.format(relationship_name, INFERRED)
        cnt_statement += ' ON CREATE SET r.{} = datetime()'.format(CREATED)
        cnt_statement += ' ON MATCH SET r.{} = datetime()'.format(UPDATED)

        result = session.run(cnt_statement, {'case_id': case_id, 'visit_id': visit_id})
        relationship_created = result.consume().counters.relationships_created
        if relationship_created > 0:
            self.relationships_created += relationship_created
            self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name,
                                                                                     0) + relationship_created
            return True
        else:
            self.log.error('Line: {}: Create (:{})-[:{}]->(:{}) relationship failed!'.format(line_num, VISIT_NODE,
                                                                                             relationship_name,
                                                                                             CASE_NODE))
            return False
