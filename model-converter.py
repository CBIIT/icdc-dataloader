#!/usr/bin/env python3

# Convert JSON scheme (from ICDC model-tool) to GraphQL schema

import argparse
import os
import sys
from icdc_schema import ICDC_Schema, PROP_TYPE
from props import Props
from bento.common.utils import check_schema_files, get_logger


def main():
    log = get_logger('Model Converter')
    # Parse arguments
    parser = argparse.ArgumentParser(description='Convert YAML schema to GraphQL schema')
    parser.add_argument("-s", "--schema", help="Schema files", action="append")
    parser.add_argument("-p", "--prop-file", help="Properties file")
    parser.add_argument("-q", "--query-file", help='Custom query file', type=argparse.FileType('r'))
    parser.add_argument("-o", "--output", help='Output GraphQL schema file name')
    args = parser.parse_args()

    # Exit if no schema files were specified or the specified files do not exist
    if not check_schema_files(args.schema, log):
        sys.exit(1)

    # Exit if no query files were specified or the specified file does not exist
    if not args.query_file:
        log.error("Query file was not specified or was unable to be read")
        sys.exit(1)

    # Log arguments
    for s in args.schema:
        log.debug("Schema file input: " + s)
    log.debug("Properties file input: " + args.prop_file)
    log.debug("Query file input: " + args.query_file.name)
    log.debug("Specified output file: " + args.output)

    # Initialize the props object with the --prop-file input
    props = Props(args.prop_file)
    log.info("Properties successfully initialized")
    # Initialize the schema the schema inputs and the props object
    schema = ICDC_Schema(args.schema, props)
    log.info("Schema successfully initialized")
    # Log number of types and relationships in output
    log.info('Types: {}, Relationships: {}'.format(schema.node_count(), schema.relationship_count()))
    # Create GraphQL schema file
    write_to_output(args, schema)
    log.info("GraphQL schema successfully generated")
    # Create documentation file from the GraphQL schema file
    copy_and_remove_tags(args.output)
    log.info("Schema documentation file successfully generated")
    # Log that the conversion has completed
    log.info("Conversion complete")


def write_to_output(args, schema):
    """
    Writes the schema object to a GraphQL formatted schema file

    :param args: The command line arguments object
    :param schema: The schema object
    """
    # Open graphql output file for writing
    with open(args.output, 'w', newline='\n') as graphql_file:
        # For each node create Graphql type definition
        for name in schema.get_node_names():
            props = schema.get_public_props_for_node(name)
            type_line = 'type {} {{'.format(name)
            print(type_line, file=graphql_file)
            for prop, propType in props.items():
                prop_type = propType[PROP_TYPE]
                if prop_type == 'DateTime' or prop_type == 'Date':
                    prop_type = 'String'
                if prop_type == 'Object' or prop_type == 'Array':
                    prop_type = 'String'
                prop_line = '  {}: {}'.format(prop, prop_type)
                print(prop_line, file=graphql_file)
            type_end = '}\n'
            print(type_end, file=graphql_file)

        # Copy custom queries from query file to graphql output
        for line in args.query_file:
            print(line, end='', file=graphql_file)


def copy_and_remove_tags(graphql_file_name):
    """
    Creates a copy of the GraphQL schema file with -doc.txt appended to the filename and removes all annotations

    :param graphql_file_name: The GraphQL schema file to be copied and reformatted
    """
    # Open GraphQL schema as read only
    graphql_file = open(graphql_file_name, 'r')
    # Create a blank file
    name, ext = os.path.splitext(graphql_file_name)
    doc_file_name = name + "-doc" + ext
    doc_file = open(doc_file_name, 'w', newline = '\n')

    # For each line containing the tag character, remove everything after the tag and then write the line to the
    # doc file. Checks the number of parenthesis to see if the tag spans multiple lines
    tag = "@"
    tag_open = False
    count_started = False
    count = 0
    for line in graphql_file:
        if tag_open:
            if not count_started:
                if "(" in line:
                    count_started = True
                    count = count_parenthesis(line)
            if count_started:
                count += count_parenthesis(line)
                if count == 0:
                    tag_open = False
                    count_started = False
        else:
            if tag in line:
                tag_open = True
                parts = line.split(tag)
                line = parts[0] + "\n"
                post_tag = parts[1]
                if "(" in post_tag:
                    count_started = True
                    count = count_parenthesis(post_tag)
                    if count == 0:
                        tag_open = False
            doc_file.write(line)


def count_parenthesis(line):
    count = line.count("(")
    count -= line.count(")")
    return count


# Call to main function
if __name__ == '__main__':
    main()
