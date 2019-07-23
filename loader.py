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

def is_validate_data(obj):
    # return {'result': False, 'message': 'Fail everything!'}
    if NODE_TYPE not in obj or ID not in obj:
        return {'result': False, 'message': "{} or {} doesn't exist!".format(NODE_TYPE, ID)}
    return {'result': True}

def cleanup_node(node):
    obj = {}
    for key, value in node.items():
        obj[key.strip()] = value.strip()
    return obj


if __name__ == '__main__':
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
    # sys.exit()

    try:
        file_list = glob.glob('{}/*.txt'.format(args.dir))
        schema = ICDC_Schema(args.schema)
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            for txt in file_list:
                log.debug("=======================")
                with open(txt) as in_file:
                    log.info('Validating file "{}" ...'.format(txt))
                    reader = csv.DictReader(in_file, delimiter='\t')
                    line_num = 0
                    for org_obj in reader:
                        obj = cleanup_node(org_obj)
                        line_num += 1
                        validate_result = is_validate_data(obj)
                        if not validate_result['result']:
                            log.critical('\nInvalid data at line {}: "{}"!'.format(line_num, validate_result['message']))
                            sys.exit(1)
                    log.info('"{}" is a valid file, loading into Neo4j ...'.format(txt))

                with open(txt) as in_file:
                    reader = csv.DictReader(in_file, delimiter='\t')
                    for org_obj in reader:
                        obj = cleanup_node(org_obj)
                        label = obj[NODE_TYPE]
                        id = obj[ID]
                        # pre_statement is used to make sure related nodes exist, create one if necessary
                        pre_statement = ''
                        # statement is used to create current node
                        statement = 'MERGE (n:{} {{{}: "{}"}}) ON CREATE '.format(label, ID, id)
                        # prop_statement set properties of current node
                        prop_statement = 'SET n.{} = "{}" '.format(ID, id)
                        # post_statement is used to create relationships between nodes
                        post_statement = ''
                        for key, value in obj.items():
                            if key in excluded_fields:
                                continue
                            elif re.match(r'\w+\.{}'.format(ID), key):
                                other_node, other_id = key.split('.')
                                relationship = schema.relationships['{}->{}'.format(label, other_node)]
                                pre_statement += 'MERGE (m:{} {{{}: "{}"}});'.format(other_node, other_id, value)
                                post_statement += 'MATCH (n:{} {{{}: "{}"}})\n'.format(label, ID, id)
                                post_statement += 'MATCH (m:{} {{{}: "{}"}})\n'.format(other_node, other_id, value)
                                post_statement += 'MERGE (n)-[:{}]->(m);'.format(relationship)
                            else:
                                log.debug('Type of {}:{} is "{}"'.format(key, value, type(value)))
                                # TODO: deal with numbers and booleans that doesn't require double quotes
                                prop_statement += ', n.{} = "{}"'.format(key, value)

                        statement += prop_statement
                        statement += ' ON MATCH ' + prop_statement + ';'

                        log.debug(pre_statement)
                        result_pre = session.run(pre_statement)
                        log.debug(result_pre)
                        log.debug(statement)
                        result = session.run(statement)
                        log.debug(result)
                        log.debug(post_statement)
                        result_post = session.run(post_statement)
                        log.debug(result_post)
        driver.close()


    except ServiceUnavailable as err:
        log.exception(err)
        log.critical("Can't connect to Neo4j server at: \"{}\"".format(uri))
