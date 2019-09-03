#!/usr/bin/env python3

import os
import csv
import re
from neo4j import  Driver, Session
from utils import *
from timeit import default_timer as timer
from icdc_schema import ICDC_Schema

NODE_TYPE = 'type'
VISIT_NODE = 'visit'
VISIT_ID = 'visit_id'
VISIT_DATE = 'visit_date'
PROP_TYPE = 'Type'
PARENT_TYPE = 'parent_type'
PARENT_ID_FIELD = 'parent_id_field'
PARENT_ID = 'parent_id'
RELATIONSHIP_NAME = 'name'
excluded_fields = {NODE_TYPE}


class DataLoader:
    def __init__(self, log, driver, schema, file_list):
        if not log:
            raise Exception('Invalid log object')
        elif not driver or not isinstance(driver, Driver):
            raise Exception('Invalid Neo4j driver object')
        elif not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        elif not file_list:
            raise Exception('Invalid file list')
        elif file_list:
            for data_file in file_list:
                if not os.path.isfile(data_file):
                    raise Exception('File "{}" doesn\'t exist'.format(data_file))
        self.log = log
        self.driver = driver
        self.schema = schema
        self.file_list = file_list

    def load(self, cheat_mode=False):
        start = timer()
        if not cheat_mode:
            validation_failed = False
            for txt in self.file_list:
                if not self.validate_file(txt):
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
        return True

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

    # Validate file
    def validate_file(self, file_name):
        with open(file_name) as in_file:
            self.log.info('Validating file "{}" ...'.format(file_name))
            reader = csv.DictReader(in_file, delimiter='\t')
            line_num = 1
            validation_failed = False
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                line_num += 1
                validate_result = self.schema.validate_node(obj[NODE_TYPE], obj)
                if not validate_result['result']:
                    self.log.error('Invalid data at line {}: "{}"!'.format(line_num, validate_result['message']))
                    validation_failed = True
            return not validation_failed

    # load file
    def load_nodes(self, session, file_name):
        self.log.info('Loading nodes from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            nodes_created = 0
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                label = obj[NODE_TYPE]
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
                                key, label, parent, combined))
                            field_name = combined

                    value_string = self.get_value_string(field_name, value)
                    if node_id:
                        prop_statement += ', n.{} = {}'.format(field_name, value_string)
                    else:
                        prop_statement.append('{}: {}'.format(field_name, value_string))

                if node_id:
                    statement += 'MERGE (n:{} {{{}: "{}"}})'.format(label, id_field, node_id)
                    statement += ' ON CREATE ' + prop_statement
                    statement += ' ON MATCH ' + prop_statement
                else:
                    statement += 'MERGE (n:{} {{ {} }})'.format(label, ', '.join(prop_statement))

                result = session.run(statement)
                count = result.summary().counters.nodes_created
                self.nodes_created += count
                nodes_created += count
                self.nodes_stat[label] = self.nodes_stat.get(label, 0) + count
            self.log.info('{} (:{}) node(s) loaded'.format(nodes_created, label))

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
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                label = obj[NODE_TYPE]
                # criteria_statement is used to find current node
                criteria_statement = self.getSearchCriteriaForNode(obj)
                relationships = []

                # Find all relationships in incoming data, and create them one by one
                for key, value in obj.items():
                    if re.match(r'\w+\.\w+', key):
                        other_node, other_id = key.split('.')
                        relationship_name = self.schema.get_relationship(label, other_node)
                        if not relationship_name:
                            self.log.error('Relationship not found!')
                            return False
                        if not self.node_exists(session, other_node, other_id, value):
                            if other_node == 'visit':
                                if self.create_visit(session, other_node, value, obj):
                                    visits_created += 1
                                    relationships.append( {PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value, RELATIONSHIP_NAME: relationship_name})
                                else:
                                    self.log.error('Couldn\'t create {} node automatically!'.format(VISIT_NODE))
                            else:
                                self.log.warning(
                                    'Node (:{} {{{}: "{}"}} not found in DB!'.format(other_node, other_id, value))
                        else:
                            relationships.append({PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value, RELATIONSHIP_NAME: relationship_name})


                for relationship in relationships:
                    relationship_name = relationship[RELATIONSHIP_NAME]
                    statement = 'MATCH (m:{} {{{}: "{}"}}) '.format(relationship[PARENT_TYPE], relationship[PARENT_ID_FIELD], relationship[PARENT_ID])
                    statement += 'MATCH (n:{} {{ {} }}) '.format(label, criteria_statement)
                    statement += 'MERGE (n)-[:{}]->(m);'.format(relationship_name)

                    result = session.run(statement)
                    count = result.summary().counters.relationships_created
                    self.relationships_created += count
                    relationships_created[relationship_name] = relationships_created.get(relationship_name, 0) + count
                    self.relationships_stat[relationship_name] = self.relationships_stat.get(relationship_name, 0) + count

            for name, count in relationships_created.items():
                self.log.info('{0} (:{2})->[:{1}]->(:{3}) relationship(s) loaded'.format(count, name, label, self.schema.get_dest_node_for_relationship(label, name)))
            if visits_created > 0:
                self.log.info('{} (:{}) node(s) loaded'.format(visits_created, VISIT_NODE))

        return True

    def getSearchCriteriaForNode(self, node):
        id_field = self.get_id_field(node)
        node_id = self.get_id(node)
        label = node[NODE_TYPE]
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
                            key, label, parent, combined))
                        field_name = combined

                else:
                    field_name = key
                criteria.append(
                    '{}: {}'.format(field_name, self.get_value_string(field_name, value)))
            criteria_statement = ', '.join(criteria)

        return criteria_statement

    def create_visit(self, session, node_type, node_id, src):
        if node_type != VISIT_NODE:
            self.log.error("Can't create (:{}) node for type: '{}'".format(VISIT_NODE, node_type))
            return False
        if not node_id:
            self.log.error("Can't create (:{}) node for id: '{}'".format(VISIT_NODE, node_id))
            return False
        if not src:
            self.log.error("Can't create (:{}) node for empty object".format(VISIT_NODE))
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
            self.log.error('Given object doesn\'t have a "{}" field!'.format(NODE_TYPE))
            return False
        source_type = src[NODE_TYPE]
        date = src[date_map[source_type]]
        if not NODE_TYPE in src:
            self.log.error('Given object doesn\'t have a "{}" field!'.format(NODE_TYPE))
            return False
        statement = 'MERGE (v:{} {{ {}: "{}", {}: "{}" }})'.format(VISIT_NODE, VISIT_ID, node_id, VISIT_DATE, date)
        result = session.run(statement)
        if result:
            count = result.summary().counters.nodes_created
            self.nodes_created += count
            self.nodes_stat[VISIT_NODE] = self.nodes_stat.get(VISIT_NODE, 0) + count
            return count > 0
        else:
            return False


