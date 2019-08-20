#!/usr/bin/env python3

import csv
import os, sys
import glob
import argparse
import re
from neo4j import GraphDatabase, ServiceUnavailable, Driver, Session
from icdc_schema import ICDC_Schema
from utils import *
from timeit import default_timer as timer

NODE_TYPE = 'type'
PSWD_ENV = 'NEO_PASSWORD'
VISIT_NODE = 'visit'
VISIT_ID = 'visit_id'
VISIT_DATE = 'visit_date'

excluded_fields = { NODE_TYPE }

class Loader:
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

    def load(self):
        start = timer()
        for txt in self.file_list:
            if not self.validate_file(txt):
                self.log.error('Validating file "{}" failed!'.format(txt))
                sys.exit(1)

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

    def get_id(self, obj):
        id_field = self.get_id_field(obj)
        if not id_field:
            return None
        if id_field not in obj:
            self.log.debug('get_id: there is no "{}" field in node, can\'t retrieve id!'.format(id_field))
            return None
        else:
            return obj[id_field]

    def is_valid_data(self, obj):
        if NODE_TYPE not in obj:
            return {'result': False, 'message': "{} doesn't exist!".format(NODE_TYPE)}

        # id = self.get_id(obj)
        # id_field = self.get_id_field(obj)
        # if id_field and not id:
        #     return {'result': False, 'message': "{} is empty".format(id_field)}

        return {'result': True}

    def cleanup_node(self, node):
        obj = {}
        for key, value in node.items():
            obj[key.strip()] = value.strip()
        return obj


    # Validate file
    def validate_file(self, file_name):
        with open(file_name) as in_file:
            self.log.info('Validating file "{}" ...'.format(file_name))
            reader = csv.DictReader(in_file, delimiter='\t')
            line_num = 0
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                line_num += 1
                validate_result = self.is_valid_data(obj)
                if not validate_result['result']:
                    self.log.error('Invalid data at line {}: "{}"!'.format(line_num, validate_result['message']))
                    return False
            return True


    # load file
    def load_nodes(self, session, file_name):
        self.log.info('Loading nodes from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            nodes_created = 0
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                label = obj[NODE_TYPE]
                id = self.get_id(obj)
                id_field = self.get_id_field(obj)
                # statement is used to create current node
                statement = ''
                # prop_statement set properties of current node
                if id:
                    prop_statement = 'SET n.{} = "{}"'.format(id_field, id)
                else:
                    prop_statement = []

                for key, value in obj.items():
                    if key in excluded_fields:
                        continue
                    elif re.match(r'\w+\.\w+', key):
                        continue
                    elif key != id_field:
                        value_string = self.get_value_string(key, value)
                        if id:
                            prop_statement += ', n.{} = {}'.format(key, value_string)
                        else:
                            prop_statement.append('{}: {}'.format(key, value_string))

                if id:
                    statement += 'MERGE (n:{} {{{}: "{}"}})'.format(label, id_field, id)
                    statement += ' ON CREATE ' + prop_statement
                    statement += ' ON MATCH ' + prop_statement
                else:
                    statement += 'MERGE (n:{} {{ {} }})'.format(label, ', '.join(prop_statement))

                self.log.debug(statement)
                result = session.run(statement)
                count = result.summary().counters.nodes_created
                self.nodes_created += count
                nodes_created += count
                self.nodes_stat[label] = self.nodes_stat.get(label, 0) + count
            self.log.info('{} (:{}) node(s) loaded'.format(nodes_created, label))

    def get_value_string(self, key, value):
        key_type = self.schema.get_type(key)
        if key_type == 'String':
            value_string = '"{}"'.format(value)
        elif key_type == 'Boolean':
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

    def node_exists(self, session, label, property, value):
        statement = 'MATCH (m:{} {{{}: "{}"}}) return m'.format(label, property, value)
        result = session.run(statement)
        count = result.detach()
        self.log.debug('{} node(s) found'.format(count))
        if count > 1:
            self.log.warning('More than one nodes found! ')
        return count >= 1

    def load_relationships(self, session, file_name):
        self.log.info('Loading relationships from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            relationships_created = 0
            visits_created = 0
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                label = obj[NODE_TYPE]
                id = self.get_id(obj)
                id_field = self.get_id_field(obj)
                # statement is used to create relationships between nodes
                statement = ''
                # condition_statement is used to find current node
                if id:
                    condition_statement = '{}: "{}"'.format(id_field, id)
                else:
                    condition_statement = []

                relationship = None
                for key, value in obj.items():
                    if key in excluded_fields:
                        continue
                    elif re.match(r'\w+\.\w+', key):
                        other_node, other_id = key.split('.')
                        relationship = self.schema.get_relationship(label, other_node)
                        if not relationship:
                            self.log.error('Relationship not found!')
                            sys.exit(1)
                        if not self.node_exists(session, other_node, other_id, value):
                            if other_node == 'visit':
                                if self.create_visit(session, other_node, value, obj):
                                    statement += 'MATCH (m:{} {{{}: "{}"}}) '.format(other_node, other_id, value)
                                    visits_created += 1
                                else:
                                    self.log.error('Couldn\'t create {} node automatically!'.format(VISIT_NODE))
                            else:
                                self.log.warning('Node (:{} {{{}: "{}"}} not found in DB!'.format(other_node, other_id, value))
                        else:
                            statement += 'MATCH (m:{} {{{}: "{}"}}) '.format(other_node, other_id, value)
                    elif not id:
                        condition_statement.append('{}: {}'.format(key, self.get_value_string(key, value)))

                if statement and relationship:
                    if id:
                        statement += 'MATCH (n:{} {{ {} }}) '.format(label, condition_statement)
                    else:
                        statement += 'MATCH (n:{} {{ {} }}) '.format(label, ', '.join(condition_statement))

                    statement += 'MERGE (n)-[:{}]->(m);'.format(relationship)

                    self.log.debug(statement)
                    result = session.run(statement)
                    count = result.summary().counters.relationships_created
                    self.relationships_created += count
                    relationships_created += count
                    self.relationships_stat[relationship] = self.relationships_stat.get(relationship, 0) + count
            self.log.info('{0} (:{2})->[:{1}]->(:{3}) relationship(s) loaded'.format(relationships_created, relationship, label, other_node))
            if visits_created > 0:
                self.log.info('{} (:{}) node(s) loaded'.format(visits_created, VISIT_NODE))

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
        self.log.debug(statement)
        result = session.run(statement)
        if result:
            count = result.summary().counters.nodes_created
            self.nodes_created += count
            self.nodes_stat[VISIT_NODE] = self.nodes_stat.get(VISIT_NODE, 0) + count
            return count > 0
        else:
            return False



def removeTrailingSlash(uri):
    if uri.endswith('/'):
        return re.sub('/+$', '', uri)
    else:
        return uri

# Data loader will try to load all TSV(.TXT) files from given directory into Neo4j
# optional arguments includes:
# -i or --uri followed by Neo4j server address and port in format like bolt://12.34.56.78:7687
def main():
    parser = argparse.ArgumentParser(description='Load TSV(TXT) files (from Pentaho) into Neo4j')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('dir', help='Data directory')

    args = parser.parse_args()
    log = get_logger('Data Loader')
    log.debug(args)

    uri = args.uri if args.uri else "bolt://localhost:7687"
    uri = removeTrailingSlash(uri)

    password = args.password
    if not password:
        if PSWD_ENV not in os.environ:
            log.error('Password not specified! Please specify password with -p or --password argument, or set {} env var'.format(PSWD_ENV))
            sys.exit(1)
        else:
            password = os.environ[PSWD_ENV]
    user = args.user if args.user else 'neo4j'

    if not args.schema:
        log.error('Please specify schema file(s) with -s or --schema argument')
        sys.exit(1)

    for schema_file in args.schema:
        if not os.path.isfile(schema_file):
            log.error('{} is not a file'.format(schema_file))
            sys.exit(1)

    if not os.path.isdir(args.dir):
        log.error('{} is not a directory'.format(args.dir))
        sys.exit(1)

    try:
        file_list = glob.glob('{}/*.txt'.format(args.dir))
        if file_list:
            schema = ICDC_Schema(args.schema)
            driver = GraphDatabase.driver(uri, auth=(user, password))
            loader = Loader(log, driver, schema, file_list)
            loader.load()

            driver.close()
        else:
            log.info('No files to load.')

    except ServiceUnavailable as err:
        log.exception(err)
        log.critical("Can't connect to Neo4j server at: \"{}\"".format(uri))

if __name__ == '__main__':
    main()
