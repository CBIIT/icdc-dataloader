#!/usr/bin/env python3

# Convert JSON scheme (from ICDC model-tool) to GraphQL schema

import os, sys
import yaml
import argparse
import inflect

NODES = 'Nodes'
RELATIONSHIPS = 'Relationships'
PROPERTIES = 'Props'
PROP_DEFINITIONS = 'PropDefinitions'
DEFAULT_TYPE = 'String'
PROP_TYPE = 'Type'
END_POINTS = 'Ends'
SRC = 'Src'
DEST = 'Dst'
CUSTOM_QUERY_FILE = 'schema-queries.graphql'
VALUE_TYPE = 'value_type'
LABEL_NEXT = 'next'

# Get type info from description
def mapType(type_name):
    mapping = {
        'string': 'String',
        'number': 'Float',
        'integer': 'Int',
        'boolean': 'Boolean',
        'array': 'Array',
        'object': 'Object',
        'datetime': 'String',
        'TBD': 'String'
    }
    result = DEFAULT_TYPE

    if type_name in mapping:
        result = mapping[type_name]
    else:
        print('Type: "{}" has no mapping, use default type: "{}"'.format(type_name, DEFAULT_TYPE))
        
    return result


def getType(name, props):
    result = DEFAULT_TYPE
    if name in props:
        prop = props[name]
        if PROP_TYPE in prop:
            prop_desc = prop[PROP_TYPE]
            if type(prop_desc) is str:
                result = mapType(prop_desc)
            elif type(prop_desc) is dict:
                if VALUE_TYPE in prop_desc:
                    result = mapType(prop_desc[VALUE_TYPE])
            else:
                print('Property type: "{}" not supported, use default type: "{}"'.format(prop_desc, DEFAULT_TYPE))

    return result

def processNode(name, desc, propDescs):
    # Gather properties
    props = {}
    if desc[PROPERTIES]:
        for prop in desc[PROPERTIES]:
            prop_type = getType(prop, propDescs)
            props[prop] = prop_type

    nodes[name] = props

def plural(word):
    plurals = {
        'program': 'programs',
        'study': 'studies',
        'study_site': 'study_sites',
        'study_arm': 'study_arms',
        'agent': 'agents',
        'cohort': 'cohorts',
        'case': 'cases',
        'demographic': 'demographics',
        'cycle': 'cycles',
        'visit': 'visits',
        'principal_investigator': 'principal_investigators',
        'diagnosis': 'diagnoses',
        'enrollment': 'enrollments',
        'prior_therapy': 'prior_therapies',
        'prior_surgery': 'prior_surgeries',
        'agent_administration': 'agent_administrations',
        'sample': 'samples',
        'evaluation': 'evaluations',
        'assay': 'assays',
        'file': 'files',
        'image': 'images',
        'physical_exam': 'physical_exams',
        'vital_signs': 'vital_signs',
        'lab_exam': 'lab_exams',
        'adverse_event': 'adverse_events',
        'disease_extent': 'disease_extents',
        'follow_up': 'follow_ups',
        'off_study': 'off_studies',
        'off_treatment': 'off_treatments'
    }
    if word in plurals:
        return plurals[word]
    else:
        print('Plural for "{}" not found!'.format(word))
        return 'NONE'

def processEdges(name, desc):
    # for key, value in desc.items():
    #     if key != END_POINTS:
    #         print('{}: {}'.format(key, value))
    if END_POINTS in desc:
        for  end_points in desc[END_POINTS]:
            src = end_points[SRC]
            dest = end_points[DEST]
            # print('{} -[:{}]-> {}'.format(src, name, dest))
            if src == dest:
                if name == 'next':
                    nodes[src]['next_{}'.format(dest)] = '[{}] @relation(name:"{}")'.format(dest, name)
                    nodes[dest]['prior_{}'.format(dest)] = '[{}] @relation(name:"{}", direction:IN)'.format(dest, name)
                else:
                    print('Self loop relationship type: "{}" unknown'.format(name))
            else:
                if src in nodes:
                    nodes[src][plural(dest)] = '[{}] @relation(name:"{}")'.format(dest, name)
                else:
                    print('Source node "{}" not found!'.format(src))
                if dest in nodes:
                    nodes[dest][plural(src)] = '[{}] @relation(name:"{}", direction:IN)'.format(src, name)
                else:
                    print('Destination node "{}" not found!'.format(dest))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert ICDC YAML schema to GraphQL schema')
    parser.add_argument('schema', help='Input YAML Schema file name')
    parser.add_argument('props', help='Input YAML Property file name')
    parser.add_argument('graphql', help='Output GraphQL schema file name')
    args = parser.parse_args()

    nodes = {}
    definitions = {}

    if os.path.isfile(args.schema):
        with open(args.schema) as schema_file, open(args.props) as props_file:
            schema = yaml.safe_load(schema_file)
            props = yaml.safe_load(props_file)[PROP_DEFINITIONS]

            for key, value in schema[NODES].items():
                # Assume all keys start with '_' are not regular nodes
                if not key.startswith('_'):
                    processNode(key, value, props)
            # print("-------------processing edges-----------------")
            for key, value in schema[RELATIONSHIPS].items():
                # Assume all keys start with '_' are not regular nodes
                if not key.startswith('_'):
                    processEdges(key, value)

    else:
        print('##### {} is not a file'.format(args.json))

    with open(args.graphql, 'w') as graphql_file:
        # Output Types
        for name in sorted(nodes.keys()):
            props = nodes[name]
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

    print('Types: {}'.format(len(nodes)))
