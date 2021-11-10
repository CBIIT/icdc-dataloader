#!/user/bin/env python3
import argparse

import os
import yaml
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from neo4j import GraphDatabase

from bento.common.utils import get_logger

logger = get_logger('ESLoader')


class ESLoader:
    def __init__(self, es_host, neo4j_driver):
        self.neo4j_driver = neo4j_driver
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
        # progress = tqdm.tqdm(unit="docs", total=number_of_docs)
        successes = 0
        total = 0
        for ok, _ in streaming_bulk(
                client=self.es_client,
                index=index_name,
                actions=self.get_data(cypher_query, mapping.keys())
        ):
            # progress.update(1)
            total += 1
            successes += 1 if ok else 0
        logger.info(f"Indexed {successes}/{total} documents")

    def load_about_page(self, index_name, mapping, file_name):
        self.recreate_index(index_name, mapping)


        logger.info('Indexing content from about page')
        if not os.path.isfile(file_name):
            raise Exception(f'"{file_name} is not a file!')
        with open(file_name) as file_obj:
            about_file = yaml.safe_load(file_obj)
            for page in about_file:
                logger.info(f'Indexing about page "{page["page"]}"')
                self.index_data(index_name, page)

    def index_data(self, index_name, object):
        self.es_client.index(index_name, body=object, id=f'page{object["page"]}')


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

    neo4j_driver = GraphDatabase.driver(
        config['neo4j_uri'],
        auth=(config['neo4j_user'], config['neo4j_password']),
        encrypted=False
    )

    loader = ESLoader(
        es_host=config['es_host'],
        neo4j_driver=neo4j_driver
    )
    for index in indices:
        if 'type' not in index or index['type'] == 'neo4j':
            loader.load(index['index_name'], index['mapping'], index['cypher_query'])
        elif index['type'] == 'about_file':
            loader.load_about_page(index['index_name'], index['mapping'], config['about_file'])
        else:
            logger.error(f'Unknown index type: "{index["type"]}"')
            continue


if __name__ == '__main__':
    main()
