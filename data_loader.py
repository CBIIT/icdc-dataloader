#!/usr/bin/env python3

import os
import csv
import re
from neo4j import  Driver, Session
from utils import *
from timeit import default_timer as timer
from icdc_schema import ICDC_Schema
from datetime import datetime, timedelta

NODE_TYPE = 'type'
VISIT_NODE = 'visit'
VISIT_ID = 'visit_id'
VISIT_DATE = 'visit_date'
PROP_TYPE = 'Type'
PARENT_TYPE = 'parent_type'
PARENT_ID_FIELD = 'parent_id_field'
PARENT_ID = 'parent_id'
RELATIONSHIP_NAME = 'name'
NODES_CREATED = 'nodes_created'
RELATIONSHIP_CREATED = 'relationship_created'
START_DATE = 'date_of_cycle_start'
END_DATE = 'date_of_cycle_end'
DATE_FORMAT = '%Y%m%d'
OF_CYCLE = 'of_cycle'
CYCLE_NODE = 'cycle'
excluded_fields = {NODE_TYPE}
CASE_NODE = 'case'
CASE_ID = 'case_id'
PREDATE = 7
FOREVER = '99991231'
INFERRED = 'inferred'



class DataLoader:
    def __init__(self, driver, schema, file_list):
        if not driver or not isinstance(driver, Driver):
            raise Exception('Invalid Neo4j driver object')
        elif not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        elif not file_list:
            raise Exception('Invalid file list')
        elif file_list:
            for data_file in file_list:
                if not os.path.isfile(data_file):
                    raise Exception('File "{}" doesn\'t exist'.format(data_file))
        self.log = get_logger('Data Loader')
        self.driver = driver
        self.schema = schema
        self.file_list = file_list

    def load(self, cheat_mode, max_violations):
        start = timer()
        if not cheat_mode:
            validation_failed = False
            for txt in self.file_list:
                if not self.validate_file(txt, max_violations) :
                    self.log.error('Validating file "{}" failed!'.format(txt))
                    validation_failed = True
            if validation_failed:
                return False
        else:
            self.log.info('Cheat mode enabled, all validations skipped!')

        self.nodes_created = 0
        self.relationships_created = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        with self.driver.session() as session:
            for txt in self.file_list:
                self.load_nodes(session, txt)
            for txt in self.file_list:
                self.load_relationships(session, txt)
        end = timer()

        # Print statistics
        for node in sorted(self.nodes_stat.keys()):
            count = self.nodes_stat[node]
            self.log.info('Node: (:{}) loaded: {}'.format(node, count))
        for rel in sorted(self.relationships_stat.keys()):
            count = self.relationships_stat[rel]
            self.log.info('Relationship: [:{}] loaded: {}'.format(rel, count))
        self.log.info('{} nodes and {} relationships loaded!'.format(self.nodes_created, self.relationships_created))
        self.log.info('Loading time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
        return {NODES_CREATED: self.nodes_created, RELATIONSHIP_CREATED: self.relationships_created}

    # Get node's id field, such as case_id for case node, or clinical_study_designation for study node
    def get_id_field(self, obj):
        if NODE_TYPE not in obj:
            self.log.error('get_id_field: there is no "{}" field in node, can\'t retrieve id!'.format(NODE_TYPE))
            return None
        node_type = obj[NODE_TYPE]
        if node_type:
            # TODO: put it somewhere in model to avoid hard coded special case for study
            if node_type == 'study':
                return 'clinical_study_designation'
            if node_type == 'program':
                return 'program_acronym'
            else:
                return node_type + '_id'
        else:
            self.log.error('get_id_field: "{}" field is empty'.format(NODE_TYPE))
            return None

    # Find node's id
    def get_id(self, obj):
        id_field = self.get_id_field(obj)
        if not id_field:
            return None
        if id_field not in obj:
            return None
        else:
            return obj[id_field]

    @staticmethod
    def cleanup_node(node):
        obj = {}
        for key, value in node.items():
            obj[key.strip()] = value.strip()
        return obj

    # Validate all cases exist in a data (TSV/TXT) file
    def validate_cases_exist_in_file(self, file_name, max_violations):
        with self.driver.session() as session:
            with open(file_name) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    obj = self.cleanup_node(org_obj)
                    line_num += 1
                    # Validate parent exist
                    if CASE_ID in obj:
                        case_id = obj[CASE_ID]
                        if not self.node_exists(session, CASE_NODE, CASE_ID, case_id):
                            self.log.error(
                                'Invalid data at line {}: Parent (:{} {{ {}: "{}" }}) doesn\'t exist!'.format(
                                    line_num, CASE_NODE, CASE_ID, case_id))
                            validation_failed = True
                            violations += 1
                            if violations >= max_violations:
                                return False
                return not validation_failed

    # Validate all parents exist in a data (TSV/TXT) file
    def validate_parents_exist_in_file(self, file_name, max_violations):
        with self.driver.session() as session:
            with open(file_name) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    obj = self.cleanup_node(org_obj)
                    node_type = obj[NODE_TYPE]
                    line_num += 1
                    # Validate parent exist
                    for key, value in obj.items():
                        if re.match(r'\w+\.\w+', key):
                            other_node, other_id = key.split('.')
                            relationship_name = self.schema.get_relationship(node_type, other_node)
                            if not relationship_name:
                                self.log.error('Relationship not found!')
                                return False
                            # Todo: create a session
                            if not self.node_exists(session, other_node, other_id, value):
                                self.log.error('Invalid data at line {}: Parent (:{} {{ {}: "{}" }}) doesn\'t exist!'.format(line_num, other_node, other_id, value))
                                validation_failed = True
                                violations += 1
                                if violations >= max_violations:
                                    return False
                return not validation_failed

    # Validate file
    def validate_file(self, file_name, max_violations):
        with open(file_name) as in_file:
            self.log.info('Validating file "{}" ...'.format(file_name))
            reader = csv.DictReader(in_file, delimiter='\t')
            line_num = 1
            validation_failed = False
            violations = 0
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                line_num += 1
                validate_result = self.schema.validate_node(obj[NODE_TYPE], obj)
                if not validate_result['result']:
                    for msg in validate_result['messages']:
                        self.log.error('Invalid data at line {}: "{}"!'.format(line_num, msg))
                    validation_failed = True
                    violations += 1
                    if violations >= max_violations:
                        return False
            return not validation_failed

    # load file
    def load_nodes(self, session, file_name):
        self.log.info('Loading nodes from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            nodes_created = 0
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                node_type = obj[NODE_TYPE]
                node_id = self.get_id(obj)
                id_field = self.get_id_field(obj)
                # statement is used to create current node
                statement = ''
                # prop_statement set properties of current node
                if node_id:
                    prop_statement = 'SET n.{} = "{}"'.format(id_field, node_id)
                else:
                    prop_statement = []

                for key, value in obj.items():
                    if key in excluded_fields:
                        continue
                    elif key == id_field:
                        continue

                    field_name = key
                    if re.match(r'\w+\.\w+', key):
                        header = key.split('.')
                        if len(header) > 2:
                            self.log.warning('Column header "{}" has multiple periods!'.format(key))
                        field_name = header[1]
                        parent = header[0]
                        combined = '{}_{}'.format(parent, field_name)
                        if field_name in obj:
                            self.log.warning('"{}" field is in both "{}" and parent "{}", use "{}" instead !'.format(
                                key, node_type, parent, combined))
                            field_name = combined

                    value_string = self.get_value_string(field_name, value)
                    if node_id:
                        prop_statement += ', n.{} = {}'.format(field_name, value_string)
                    else:
                        prop_statement.append('{}: {}'.format(field_name, value_string))

                if node_id:
                    statement += 'MERGE (n:{} {{{}: "{}"}})'.format(node_type, id_field, node_id)
                    statement += ' ON CREATE ' + prop_statement
                    statement += ' ON MATCH ' + prop_statement
                else:
                    statement += 'MERGE (n:{} {{ {} }})'.format(node_type, ', '.join(prop_statement))

                result = session.run(statement)
                count = result.summary().counters.nodes_created
                self.nodes_created += count
                nodes_created += count
                self.nodes_stat[node_type] = self.nodes_stat.get(node_type, 0) + count
            self.log.info('{} (:{}) node(s) loaded'.format(nodes_created, node_type))

    def get_value_string(self, key, value):
        key_type = self.schema.get_type(key)
        if key_type[PROP_TYPE] == 'String':
            value_string = '"{}"'.format(value)
        elif key_type[PROP_TYPE] == 'Boolean':
            cleaned_value = None
            if re.search(r'yes|true', value, re.IGNORECASE):
                cleaned_value = True
            elif re.search(r'no|false', value, re.IGNORECASE):
                cleaned_value = False
            else:
                self.log.debug('Unsupported Boolean value: "{}"'.format(value))
                cleaned_value = None
            if cleaned_value != None:
                value_string = '{}'.format(cleaned_value)
            else:
                value_string = '""'
        else:
            value_string = value if value else 0
        return value_string

    def node_exists(self, session, label, prop, value):
        statement = 'MATCH (m:{} {{{}: "{}"}}) return m'.format(label, prop, value)
        result = session.run(statement)
        count = result.detach()
        if count > 1:
            self.log.warning('More than one nodes found! ')
        return count >= 1

    def load_relationships(self, session, file_name):
        self.log.info('Loading relationships from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            relationships_created = {}
            visits_created = 0
            line_num = 1
            for org_obj in reader:
                line_num += 1
                obj = self.cleanup_node(org_obj)
                node_type = obj[NODE_TYPE]
                # criteria_statement is used to find current node
                criteria_statement = self.getSearchCriteriaForNode(obj)
                relationships = []

                # Find all relationships in incoming data, and create them one by one
                for key, value in obj.items():
                    if re.match(r'\w+\.\w+', key):
                        other_node, other_id = key.split('.')
                        relationship_name = self.schema.get_relationship(node_type, other_node)
                        if not relationship_name:
                            self.log.error('Line: {}: Relationship not found!'.format(line_num))
                            return False
                        if not self.node_exists(session, other_node, other_id, value):
                            if other_node == 'visit':
                                if self.create_visit(session, line_num, other_node, value, obj):
                                    visits_created += 1
                                    relationships.append( {PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value, RELATIONSHIP_NAME: relationship_name})
                                else:
                                    self.log.error('Line: {}: Couldn\'t create {} node automatically!'.format(line_num, VISIT_NODE))
                            else:
                                self.log.warning(
                                    'Node (:{} {{{}: "{}"}} not found in DB!'.format(other_node, other_id, value))
                        else:
                            relationships.append({PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value, RELATIONSHIP_NAME: relationship_name})


                for relationship in relationships:
                    relationship_name = relationship[RELATIONSHIP_NAME]
                    parent_node = relationship[PARENT_TYPE]
                    statement = 'MATCH (m:{} {{{}: "{}"}}) '.format(parent_node, relationship[PARENT_ID_FIELD], relationship[PARENT_ID])
                    statement += 'MATCH (n:{} {{ {} }}) '.format(node_type, criteria_statement)
                    statement += 'MERGE (n)-[:{}]->(m);'.format(relationship_name)

                    result = session.run(statement)
                    count = result.summary().counters.relationships_created
                    self.relationships_created += count
                    relationship_pattern = '(:{})->[:{}]->(:{})'.format(node_type, relationship_name, parent_node)
                    relationships_created[relationship_pattern] = relationships_created.get(relationship_pattern, 0) + count
                    self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + count

            for rel, count in relationships_created.items():
                self.log.info('{} {} relationship(s) loaded'.format(count, rel))
            if visits_created > 0:
                self.log.info('{} (:{}) node(s) loaded'.format(visits_created, VISIT_NODE))

        return True

    def getSearchCriteriaForNode(self, node):
        id_field = self.get_id_field(node)
        node_id = self.get_id(node)
        node_type = node[NODE_TYPE]
        if node_id:
            criteria_statement = '{}: "{}"'.format(id_field, node_id)
        else:
            criteria = []
            for key, value in node.items():
                if key in excluded_fields:
                    continue
                if key == id_field:
                    continue
                if re.match(r'\w+\.\w+', key):
                    # Add parent id to search conditions
                    header = key.split('.')
                    if len(header) > 2:
                        self.log.warning('Column header "{}" has multiple periods!'.format(key))
                    field_name = header[1]
                    parent = header[0]
                    combined = '{}_{}'.format(parent, field_name)
                    if field_name in node:
                        self.log.warning('"{}" field is in both "{}" and parent "{}", use "{}" instead !'.format(
                            key, node_type, parent, combined))
                        field_name = combined

                else:
                    field_name = key
                criteria.append(
                    '{}: {}'.format(field_name, self.get_value_string(field_name, value)))
            criteria_statement = ', '.join(criteria)

        return criteria_statement

    def create_visit(self, session, line_num, node_type, node_id, src):
        if node_type != VISIT_NODE:
            self.log.error("Line: {}: Can't create (:{}) node for type: '{}'".format(line_num, VISIT_NODE, node_type))
            return False
        if not node_id:
            self.log.error("Line: {}: Can't create (:{}) node for id: '{}'".format(line_num, VISIT_NODE, node_id))
            return False
        if not src:
            self.log.error("Line: {}: Can't create (:{}) node for empty object".format(line_num, VISIT_NODE))
            return False
        if not session or not isinstance(session, Session):
            self.log.error("Neo4j session is not valid!")
            return False
        date_map = {
            'vital_signs': 'date_of_vital_signs',
            'physical_exam': 'date_of_examination',
            'disease_extent': 'date_of_evaluation'
        }
        if not NODE_TYPE in src:
            self.log.error('Line: {}: Given object doesn\'t have a "{}" field!'.format(line_num, NODE_TYPE))
            return False
        source_type = src[NODE_TYPE]
        date = src[date_map[source_type]]
        if not date:
            self.log.error('Line: {}: Visit date is empty!'.format(line_num))
            return False
        if not NODE_TYPE in src:
            self.log.error('Line: {}: Given object doesn\'t have a "{}" field!'.format(line_num, NODE_TYPE))
            return False
        statement = 'MERGE (v:{} {{ {}: "{}", {}: "{}", {}: true }})'.format(VISIT_NODE, VISIT_ID, node_id, VISIT_DATE, date, INFERRED)
        result = session.run(statement)
        if result:
            count = result.summary().counters.nodes_created
            self.nodes_created += count
            self.nodes_stat[VISIT_NODE] = self.nodes_stat.get(VISIT_NODE, 0) + count
            if count > 0:
                case_id = src[CASE_ID]
                if not self.connect_visit_to_cycle(session, line_num, node_id, case_id, date):
                    self.log.error('Line: {}: Visit: "{}" does NOT belong to a cycle!'.format(line_num, node_id))
                return True
        else:
            return False

    def connect_visit_to_cycle(self, session, line_num, visit_id, case_id, visit_date):
        find_cycles_stmt = 'MATCH (c:cycle) WHERE c.case_id = "{}" RETURN c ORDER BY c.date_of_cycle_start'.format(case_id)
        result = session.run(find_cycles_stmt)
        if result:
            first_date = None
            pre_date = None
            relationship_name = self.schema.get_relationship(VISIT_NODE, CYCLE_NODE)
            if not relationship_name:
                return False
            for record in result.records():
                cycle = record.data()['c']
                date = datetime.strptime(visit_date, DATE_FORMAT)
                start_date = datetime.strptime(cycle[START_DATE], DATE_FORMAT)
                if not first_date:
                    first_date = start_date
                    pre_date = first_date - timedelta(days=PREDATE)
                if cycle[END_DATE]:
                    end_date = datetime.strptime(cycle[END_DATE], DATE_FORMAT)
                else:
                    self.log.warning('Line: {}: No end dates for cycle started on {} for {}'.format(line_num, start_date.strftime(DATE_FORMAT), case_id))
                    end_date = datetime.strptime(FOREVER, DATE_FORMAT)
                if (date >= start_date and date <= end_date) or (date < first_date and date >= pre_date):
                    if date < first_date and date >= pre_date:
                        self.log.info('Line: {}: Date: {} is before first cycle, but within {} days before first cycle started: {}, connected to first cycle'.format(line_num, visit_date, PREDATE, first_date.strftime(DATE_FORMAT)))
                    cycle_id = cycle.id
                    connect_stmt = 'MATCH (v:{} {{{}: "{}"}}) MATCH (c:{}) WHERE id(c) = {} MERGE (v)-[:{} {{ {}: true }}]->(c)'.format(VISIT_NODE, VISIT_ID, visit_id, CYCLE_NODE, cycle_id, relationship_name, INFERRED)
                    cnt_result = session.run(connect_stmt)
                    relationship_created = cnt_result.summary().counters.relationships_created
                    if relationship_created > 0:
                        self.relationships_created += relationship_created
                        self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + relationship_created
                        return True
                    else:
                        self.log.error('Line: {}: Create (:visit)-[:of_cycle]->(:cycle) relationship failed!'.format(line_num))
                        return False
            return self.connect_visit_to_case(session, line_num, visit_id, case_id)
        else:
            self.log.error('Line: {}: No cycles found for case: {}'.format(line_num, case_id))
            return False

    def connect_visit_to_case(self, session, line_num, visit_id, case_id):
        relationship_name = self.schema.get_relationship(VISIT_NODE, CASE_NODE)
        if not relationship_name:
            return False
        cnt_statement = 'MATCH (c:case {{ case_id: "{}"}}) MATCH (v:visit {{ {}: "{}" }}) '.format(case_id, VISIT_ID, visit_id)
        cnt_statement += 'MERGE (c)<-[:{} {{ {}: true }}]-(v)'.format(relationship_name, INFERRED)
        result = session.run(cnt_statement)
        relationship_created = result.summary().counters.relationships_created
        if relationship_created > 0:
            self.relationships_created += relationship_created
            self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + relationship_created
            return True
        else:
            self.log.error('Line: {}: Create (:{})-[:{}]->(:{}) relationship failed!'.format(line_num, VISIT_NODE, relationship_name, CASE_NODE))
            return False


