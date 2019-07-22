#!/usr/bin/env python3

import csv
import os, sys
import glob
import argparse
import re
from neo4j import GraphDatabase, ServiceUnavailable
from icdc_schema import ICDC_Schema

NODE_TYPE = 'type'
ID = 'submitter_id'

excluded_fields = { NODE_TYPE }

def is_validate_data(obj):
    # return {'result': False, 'message': 'Fail everything!'}
    if NODE_TYPE not in obj or ID not in obj:
        return {'result': False, 'message': "{} or {} doesn't exist!".format(NODE_TYPE, ID)}
    return {'result': True}

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

    print(args)
    # sys.exit()

    try:
        file_list = glob.glob('{}/*.txt'.format(args.dir))
        schema = ICDC_Schema(args.schema)
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            for txt in file_list:
                print("=======================")
                with open(txt) as in_file:
                    print('Validating file "{}" ...'.format(txt), end='')
                    reader = csv.DictReader(in_file, delimiter='\t')
                    line_num = 0
                    for org_obj in reader:
                        obj = {key.strip():value.strip() for (key,value) in org_obj.items()}
                        line_num += 1
                        validate_result = is_validate_data(obj)
                        if not validate_result['result']:
                            print('\nInvalid data at line {}: "{}"!'.format(line_num, validate_result['message']))
                            sys.exit(1)
                    print('Done\n"{}" is a valid file, loading into Neo4j ...'.format(txt), end='')

                with open(txt) as in_file:
                    reader = csv.DictReader(in_file, delimiter='\t')
                    for org_obj in reader:
                        obj = {key.strip():value.strip() for (key,value) in org_obj.items()}
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
                                # print('Type of {}:{} is "{}"'.format(key, value, type(value)))
                                # TODO: deal with numbers and booleans that doesn't require double quotes
                                prop_statement += ', n.{} = "{}"'.format(key, value)

                        statement += prop_statement
                        statement += ' ON MATCH ' + prop_statement + ';'

                        # print(pre_statement)
                        result = session.run(pre_statement)
                        # print(result)
                        # print(statement)
                        result = session.run(statement)
                        # print(result)
                        # print(after_statement)
                        result = session.run(post_statement)
                        # print(result)
                    print('Done.')
        driver.close()


    except ServiceUnavailable as err:
        print(err)
        print("Can't connect to Neo4j server at: \"{}\"".format(uri))
