#!/usr/bin/env python3

# Convert JSON scheme (from ICDC model-tool) to GraphQL schema

import argparse
import sys
from lib.icdc_schema import ICDC_Schema, PROP_TYPE
from lib.utils import check_schema_files, get_logger


if __name__ == '__main__':
    log = get_logger('Model Converter')
    parser = argparse.ArgumentParser(description='Convert ICDC YAML schema to GraphQL schema')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('query_file', help='Custom query file', type=argparse.FileType('r'))

    parser.add_argument('graphql', help='Output GraphQL schema file name')
    args = parser.parse_args()

    if not check_schema_files(args.schema, log):
        sys.exit(1)

    schema = ICDC_Schema(args.schema)

    if not args.query_file:
        log.error('Read custom query file "{}" failed!'.format(args.query_file))
        sys.exit(1)

    with open(args.graphql, 'w') as graphql_file:
        # Output Types
        for name in schema.get_node_names():
            props = schema.get_props_for_node(name)
            typeLine = 'type {} {{'.format(name)
            print(typeLine)
            print(typeLine, file=graphql_file)
            for prop, propType in props.items():
                prop_type = propType[PROP_TYPE]
                if prop_type == 'DateTime' or prop_type == 'Date':
                    prop_type = 'String'
                propLine = '  {}: {}'.format(prop, prop_type)
                print(propLine)
                print(propLine, file=graphql_file)
            typeEnd = '}\n'
            print(typeEnd)
            print(typeEnd, file=graphql_file)

        # Copy custom queries
        for line in args.query_file:
            print(line, end='', file=graphql_file)

    print('Types: {}, Relationships: {}'.format(schema.node_count(), schema.relationship_count()))
