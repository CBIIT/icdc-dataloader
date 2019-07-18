#!/usr/bin/env python3

# Convert JSON scheme (from ICDC model-tool) to GraphQL schema

import argparse
from icdc_schema import *


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert ICDC YAML schema to GraphQL schema')
    parser.add_argument('schema', help='Input YAML Schema file name')
    parser.add_argument('props', help='Input YAML Property file name')
    parser.add_argument('graphql', help='Output GraphQL schema file name')
    args = parser.parse_args()

    schema = ICDC_Schema((args.schema, args.props))

    with open(args.graphql, 'w') as graphql_file:
        # Output Types
        for name in sorted(schema.nodes.keys()):
            props = schema.nodes[name]
            typeLine = 'type {} {{'.format(name)
            print(typeLine)
            print(typeLine, file=graphql_file)
            for prop, propType in props.items():
                propLine = '  {}: {}'.format(prop, propType)
                print(propLine)
                print(propLine, file=graphql_file)
            typeEnd = '}\n'
            print(typeEnd)
            print(typeEnd, file=graphql_file)

        # Copy custom queries
        with open(CUSTOM_QUERY_FILE) as query_file:
            for line in query_file:
                print(line, end='', file=graphql_file)

    print('Types: {}, Relationships: {}'.format(len(schema.nodes), schema.numRelationships))
