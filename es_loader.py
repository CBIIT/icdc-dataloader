#!/usr/bin/env python3
import argparse

import os
import yaml
import re
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import streaming_bulk
from requests_aws4auth import AWS4Auth
from botocore.session import Session
from neo4j import GraphDatabase

from bento.common.utils import get_logger, print_config
from icdc_schema import ICDC_Schema, PROPERTIES, ENUM, PROP_ENUM, PROP_TYPE, REQUIRED, DESCRIPTION
from props import Props

logger = get_logger('ESLoader')
OPENSEARCH_DATA = 'opensearch_data'


class ESLoader:
    def __init__(self, es_host, neo4j_driver):
        self.neo4j_driver = neo4j_driver
        timeout_seconds = 60
        if 'amazonaws.com' in es_host:
            awsauth = AWS4Auth(
                refreshable_credentials=Session().get_credentials(),
                region='us-east-1',
                service='es'
            )
            self.es_client = Elasticsearch(
                hosts=[es_host],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=timeout_seconds
            )
        else:
            self.es_client = Elasticsearch(hosts=[es_host], timeout=timeout_seconds)

    def create_index(self, index_name, mapping):
        """Creates an index in Elasticsearch if one isn't already there."""
        return self.es_client.indices.create(
            index=index_name,
            body={
                "settings": {
                    "number_of_shards": 1,
                    "index.mapping.nested_objects.limit": 100000
                },
                "mappings": {
                    "properties": mapping
                },
            },
            ignore=400,
        )

    def delete_index(self, index_name):
        return self.es_client.indices.delete(index=index_name, ignore_unavailable=True)

    def get_data(self, cypher_query: str, fields: dict, skip: int = 0, limit: int = 10000000):
        """Reads data from Neo4j, for each row
        yields a single document. This function is passed into the bulk()
        helper to create many documents in sequence.
        """
        with self.neo4j_driver.session() as session:
            result = session.run(cypher_query, {"skip": skip, "limit": limit})
            for record in result:
                keys = record.keys()
                if len(keys) == 1 and keys[0].lower() == OPENSEARCH_DATA.lower():
                    record = record[record.keys()[0]]
                doc = {}
                for key in fields:
                    doc[key] = record[key]
                yield doc

    def recreate_index(self, index_name, mapping):
        logger.info(f'Deleting old index: "{index_name}"')
        result = self.delete_index(index_name)
        logger.info(result)

        logger.info(f'Creating index: "{index_name}"')
        result = self.create_index(index_name, mapping)
        logger.info(result)

    def load(self, index_name, mapping, cypher_queries):
        self.recreate_index(index_name, mapping)
        logger.info('Indexing data from Neo4j')
        total_successes = 0
        total_documents = 0
        for i, cypher_query in enumerate(cypher_queries):
            query = cypher_query.get('query')
            if query is None:
                raise Exception(f'A query entry is missing for {index_name}')
            page_size = cypher_query.get('page_size')
            if page_size is None:
                page_size = 0
            logger.info(f'Executing index query {i+1}/{len(cypher_queries)}')
            if page_size > 0:
                logger.info(f'Page size is set to {page_size}')
                skip = 0
                total = page_size
                while total == page_size:
                    successes, total = self.bulk_load(
                        index_name,
                        self.get_data(
                            query, mapping.keys(), skip=skip, limit=page_size
                        )
                    )
                    total_successes += successes
                    total_documents += total
                    logger.info(f"Indexing in progress: successfully indexed {total_successes}/{total_documents} documents")
                    skip += page_size
            else:
                logger.info(f'Pagination is disabled')
                successes, documents = self.bulk_load(index_name, self.get_data(query, mapping.keys()))
                total_successes += successes
                total_documents += documents
        logger.info(f"Indexing completed: successfully indexed {total_successes}/{total_documents} documents")
        return total_successes

    def bulk_load(self, index_name, data):
        successes = 0
        total = 0
        for ok, _ in streaming_bulk(
                client=self.es_client,
                index=index_name,
                actions=data,
                max_retries=2,
                initial_backoff=10,
                max_backoff=20,
                max_chunk_bytes=10485760
        ):
            total += 1
            successes += 1 if ok else 0
        return successes, total

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

    summary = {}
    indices_list = config.get('indices_list')
    if indices_list is not None:
        lower_indices_list = [item.lower() for item in indices_list]
        logger.warning(f"An indices list is provided, only the indices in the indices list {indices_list} will be loaded")
    index_name_list = []
    for index in indices:
        index_name = index.get('index_name')
        index_name_list.append(index_name.lower())
        if indices_list is not None and index_name is not None:
            lower_index_name = index_name.lower()
            if lower_index_name not in lower_indices_list:
                continue
        summary[index_name] = "ERROR!"
        logger.info(f'Begin loading index: "{index_name}"')
        if 'type' not in index or index['type'] == 'neo4j':
            cypher_queries = index.get('cypher_queries')
            cypher_query = index.get('cypher_query')
            if cypher_queries is None and cypher_query is not None:
                cypher_queries = [{'query': cypher_query}]
            try:
                _validate_cypher_queries(cypher_queries)
                summary[index_name] = loader.load(index_name, index['mapping'], cypher_queries)
            except Exception as ex:
                logger.error(f'There is an error in the "{index_name}" index definition, this index will not be loaded')
                logger.error(ex)
        elif index['type'] == 'about_file':
            if 'about_file' in config:
                loader.load_about_page(index_name, index['mapping'], config['about_file'])
                summary[index_name] = "Loaded Successfully"
            else:
                logger.warning(f'"about_file" not set in configuration file, {index_name} will not be loaded!')
        elif index['type'] == 'model':
            if load_model and 'subtype' in index:
                loader.load_model(index_name, index['mapping'], index['subtype'])
                summary[index_name] = "Loaded Successfully"
            else:
                logger.warning(
                    f'"model_files" not set in configuration file, {index_name} will not be loaded!')
        else:
            logger.error(f'Unknown index type: "{index["type"]}"')
    if indices_list is not None:
        for indices_name in indices_list:
            if indices_name.lower() not in index_name_list:
                logger.warning(f'The index {indices_name} in the indices list does not exist in the definition file')
    logger.info(f'Index loading summary:')
    for index in summary.keys():
        logger.info(f'{index}: {summary[index]}')


def _validate_cypher_queries(cypher_queries):
    if type(cypher_queries) is not list:
        raise Exception(f'The required property "cypher_queries" must be a list')
    for i, cypher_query in enumerate(cypher_queries):
        if type(cypher_query) is not dict:
            raise Exception(f'Each entry in the "cypher_queries" list be a dict with a "query" property')
        query = cypher_query.get('query')
        if query is None:
            raise Exception(f'The required property "query" is missing from a "cypher_queries" entry')
        page_size = cypher_query.get('page_size')
        if not _check_query_for_pagination(query):
            logger.warning(f'Pagination parameters are missing from "cypher_queries" entry {i+1}, pagination will be disabled for this query')
            cypher_query['page_size'] = 0
        elif page_size is None:
            logger.warning(
                f'The page_size property is missing from "cypher_queries" entry {i+1}, pagination will be disabled for this query')
            cypher_query['page_size'] = 0


def _check_query_for_pagination(query: str):
    match = re.search('skip\s*\$skip\s*limit\s*\$limit', query, re.IGNORECASE)
    return match is not None


if __name__ == '__main__':
    main()
