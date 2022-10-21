#!/usr/bin/env python3
import argparse

import os
import yaml
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import streaming_bulk
from requests_aws4auth import AWS4Auth
from botocore.session import Session
from neo4j import GraphDatabase

from bento.common.utils import get_logger, print_config
from icdc_schema import ICDC_Schema, PROPERTIES, ENUM, PROP_ENUM, PROP_TYPE, REQUIRED, DESCRIPTION
from props import Props

logger = get_logger('ESLoader')


class ESLoader:
    def __init__(self, es_host, neo4j_driver):
        self.neo4j_driver = neo4j_driver
        if 'amazonaws.com' in es_host:
            awsauth = AWS4Auth(
                refreshable_credentials=Session().get_credentials(),
                region='us-east-1',
                service='es'
            )
            self.es_client = Elasticsearch(
                hosts=[es_host],
                http_auth = awsauth,
                use_ssl = True,
                verify_certs = True,
                connection_class = RequestsHttpConnection
            )
        else:
            self.es_client = Elasticsearch(hosts=[es_host])

    def create_index(self, index_name, mapping):
        """Creates an index in Elasticsearch if one isn't already there."""
        return self.es_client.indices.create(
            index=index_name,
            body={
                "settings": {"number_of_shards": 1},
                "mappings": {
                    "properties": mapping
                },
            },
            ignore=400,
        )

    def delete_index(self, index_name):
        return self.es_client.indices.delete(index=index_name, ignore_unavailable=True)

    def get_data(self, cypher_query, fields):
        """Reads data from Neo4j, for each row
        yields a single document. This function is passed into the bulk()
        helper to create many documents in sequence.
        """
        with self.neo4j_driver.session() as session:
            result = session.run(cypher_query)
            for record in result:
                doc = {}
                for key in fields:
                    doc[key] = record[key]
                yield doc

    def recreate_index(self, index_name, mapping):
        logger.info(f'Deleting old index "{index_name}"')
        result = self.delete_index(index_name)
        logger.info(result)

        logger.info(f'Creating index "{index_name}"')
        result = self.create_index(index_name, mapping)
        logger.info(result)

    def load(self, index_name, mapping, cypher_query):
        self.recreate_index(index_name, mapping)

        logger.info('Indexing data from Neo4j')
        self.bulk_load(index_name, self.get_data(cypher_query, mapping.keys()))

    def bulk_load(self, index_name, data):
        logger.info('Indexing data in bulk ...')

        successes = 0
        total = 0
        for ok, _ in streaming_bulk(
                client=self.es_client,
                index=index_name,
                actions=data
        ):
            total += 1
            successes += 1 if ok else 0
        logger.info(f"Indexed {successes}/{total} documents")

    def load_about_page(self, index_name, mapping, file_name):
        logger.info('Indexing content from about page')
        if not os.path.isfile(file_name):
            raise Exception(f'"{file_name} is not a file!')

        self.recreate_index(index_name, mapping)
        with open(file_name) as file_obj:
            about_file = yaml.safe_load(file_obj)
            for page in about_file:
                logger.info(f'Indexing about page "{page["page"]}"')
                self.index_data(index_name, page, f'page{page["page"]}')

    def read_model(self, model_files, prop_file):
        for file_name in model_files:
            if not os.path.isfile(file_name):
                raise Exception(f'"{file_name} is not a file!')
        if not os.path.isfile(prop_file):
            raise Exception(f'"{prop_file} is not a file!')

        self.model = ICDC_Schema(model_files, Props(prop_file))

    def load_model(self, index_name, mapping, subtype):
        logger.info(f'Indexing data model')
        if not self.model:
            logger.warning(f'Data model is not loaded, {index_name} will not be loaded!')
            return

        self.recreate_index(index_name, mapping)
        self.bulk_load(index_name, self.get_model_data(subtype))

    def get_model_data(self, subtype):
        nodes = self.model.nodes
        for node_name, obj in nodes.items():
            props = obj[PROPERTIES]
            if subtype == 'node':
                yield {
                    'type': 'node',
                    'node': node_name,
                    'node_name': node_name,
                    'node_kw': node_name
                }
            else:
                for prop_name, prop in props.items():
                    # Skip relationship based properties
                    if "@relation" in obj[PROPERTIES][prop_name][PROP_TYPE]:
                        continue
                    if subtype == 'property':
                        yield {
                            'type': 'property',
                            'node': node_name,
                            'node_name': node_name,
                            'property': prop_name,
                            'property_name': prop_name,
                            'property_kw': prop_name,
                            'property_description': prop.get(DESCRIPTION, ''),
                            'property_required': prop.get(REQUIRED, False),
                            'property_type': PROP_ENUM if ENUM in prop else prop[PROP_TYPE]
                        }
                    elif subtype == 'value' and ENUM in prop:
                        for value in prop[ENUM]:
                            yield {
                                    'type': 'value',
                                    "node": node_name,
                                    "node_name": node_name,
                                    "property": prop_name,
                                    "property_name": prop_name,
                                    'property_description': prop.get(DESCRIPTION, ''),
                                    'property_required': prop.get(REQUIRED, False),
                                    'property_type': PROP_ENUM,
                                    "value": value,
                                    "value_kw": value
                            }

    def index_data(self, index_name, object, id):
        self.es_client.index(index_name, body=object, id=id)



def main():
    parser = argparse.ArgumentParser(description='Load data from Neo4j to Elasticsearch')
    parser.add_argument('indices_file',
                        type=argparse.FileType('r'),
                        help='Configuration file for indices, example is in config/es_indices.example.yml')
    parser.add_argument('config_file',
                        type=argparse.FileType('r'),
                        help='Configuration file, example is in config/es_loader.example.yml')
    args = parser.parse_args()

    config = yaml.safe_load(args.config_file)['Config']
    indices = yaml.safe_load(args.indices_file)['Indices']
    print_config(logger, config)

    neo4j_driver = GraphDatabase.driver(
        config['neo4j_uri'],
        auth=(config['neo4j_user'], config['neo4j_password']),
        encrypted=False
    )

    loader = ESLoader(
        es_host=config['es_host'],
        neo4j_driver=neo4j_driver
    )


    load_model = False
    if 'model_files' in config and config['model_files'] and 'prop_file' in config and config['prop_file']:
        loader.read_model(config['model_files'], config['prop_file'])
        load_model = True

    for index in indices:
        if 'type' not in index or index['type'] == 'neo4j':
            loader.load(index['index_name'], index['mapping'], index['cypher_query'])
        elif index['type'] == 'about_file':
            if 'about_file' in config:
                loader.load_about_page(index['index_name'], index['mapping'], config['about_file'])
            else:
                logger.warning(f'"about_file" not set in configuration file, {index["index_name"]} will not be loaded!')
        elif index['type'] == 'model':
            if load_model and 'subtype' in index:
                loader.load_model(index['index_name'], index['mapping'], index['subtype'])
            else:
                logger.warning(f'"model_files" not set in configuration file, {index["index_name"]} will not be loaded!')
        else:
            logger.error(f'Unknown index type: "{index["type"]}"')
            continue


if __name__ == '__main__':
    main()
