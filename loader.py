#!/usr/bin/env python3

import csv
import os, sys
import glob
import argparse
import re
from neo4j import GraphDatabase, ServiceUnavailable
from icdc_schema import ICDC_Schema
from utils import *

NODE_TYPE = 'type'
ID = 'submitter_id'

excluded_fields = { NODE_TYPE }

class Loader:
    def __init__(self, log, driver, schema, file_list):
        self.log = log
        self.driver = driver
        self.schema = schema
        self.file_list = file_list

    def load(self):
        for txt in self.file_list:
            self.validate_file(txt)

        self.nodes_created = 0
        self.relationships_created = 0
        with self.driver.session() as session:
            for txt in self.file_list:
                self.load_nodes(session, txt)
            for txt in self.file_list:
                self.load_relationships(session, txt)
        self.log.info('{} nodes and {} relationships created!'.format(self.nodes_created, self.relationships_created))


    def is_validate_data(self, obj):
        # return {'result': False, 'message': 'Fail everything!'}
        if NODE_TYPE not in obj or ID not in obj:
            return {'result': False, 'message': "{} or {} doesn't exist!".format(NODE_TYPE, ID)}
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
                validate_result = self.is_validate_data(obj)
                if not validate_result['result']:
                    self.log.critical('\nInvalid data at line {}: "{}"!'.format(line_num, validate_result['message']))
                    return False
            return True


    # load file
    def load_nodes(self, session, file_name):
        self.log.info('Loading nodes from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                label = obj[NODE_TYPE]
                id = obj[ID]
                # statement is used to create current node
                statement = 'MERGE (n:{} {{{}: "{}"}}) ON CREATE '.format(label, ID, id)
                # prop_statement set properties of current node
                prop_statement = 'SET n.{} = "{}" '.format(ID, id)
                # post_statement is used to create relationships between nodes
                for key, value in obj.items():
                    if key in excluded_fields:
                        continue
                    elif re.match(r'\w+\.{}'.format(ID), key):
                        other_node, other_id = key.split('.')
                    elif key != ID:
                        # log.debug('Type of {}:{} is "{}"'.format(key, value, type(value)))
                        # TODO: deal with numbers and booleans that doesn't require double quotes
                        prop_statement += ', n.{} = "{}"'.format(key, value)

                statement += prop_statement
                statement += ' ON MATCH ' + prop_statement + ';'

                self.log.debug(statement)
                result = session.run(statement)
                count = result.summary().counters.nodes_created
                self.nodes_created += count
                self.log.debug(count)

    def load_relationships(self, session, file_name):
        self.log.info('Loading relationships from file: {}'.format(file_name))

        with open(file_name) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            for org_obj in reader:
                obj = self.cleanup_node(org_obj)
                label = obj[NODE_TYPE]
                id = obj[ID]
                # post_statement is used to create relationships between nodes
                statement = ''
                for key, value in obj.items():
                    if key in excluded_fields:
                        continue
                    elif re.match(r'\w+\.{}'.format(ID), key):
                        other_node, other_id = key.split('.')
                        statement += 'MATCH (n:{} {{{}: "{}"}})\n'.format(label, ID, id)
                        statement += 'MATCH (m:{} {{{}: "{}"}})\n'.format(other_node, other_id, value)
                        statement += 'MERGE (n)-[:{}]->(m);'.format(relationship)
                        relationship = self.schema.get_relationship(label, other_node)

                self.log.debug(statement)
                result = session.run(statement)
                count = result.summary().counters.relationships_created
                self.relationships_created += count
                self.log.debug(count)

def main():
    parser = argparse.ArgumentParser(description='Load TSV(TXT) files (from Pentaho) to Neo4j')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('dir', help='Data directory')

    args = parser.parse_args()

    uri = args.uri if args.uri else "bolt://localhost:7687"
    password = args.password if args.password else os.environ['NEO_PASSWORD']
    user = args.user if args.user else 'neo4j'

    log = get_logger('Data Loader')

    log.debug(args)

    try:
        file_list = glob.glob('{}/*.txt'.format(args.dir))
        schema = ICDC_Schema(args.schema)
        driver = GraphDatabase.driver(uri, auth=(user, password))
        loader = Loader(log, driver, schema, file_list)
        loader.load()

        driver.close()

    except ServiceUnavailable as err:
        log.exception(err)
        log.critical("Can't connect to Neo4j server at: \"{}\"".format(uri))

if __name__ == '__main__':
    main()
