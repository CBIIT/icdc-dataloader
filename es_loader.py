#!/user/bin/env python3
import os
import argparse

from neo4j import GraphDatabase
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import yaml
import tqdm

from bento.common.utils import get_logger

logger = get_logger('ESLoader')

class ESLoader:
    def __init__(self, es_host, neo4j_driver, index_name, mapping, cypher_query):
        self.neo4j_driver = neo4j_driver
        self.es_client = Elasticsearch(hosts=[es_host])
        self.index_name = index_name
        self.mapping = mapping
        self.cypher_query = cypher_query

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
        return self.es_client.indices.delete(index=index_name)

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

    def load(self):
        logger.info('Deleting index')
        result = self.delete_index(self.index_name)
        logger.info(result)

        logger.info(f'Creating index "{self.index_name}"')
        result = self.create_index(self.index_name, self.mapping)
        logger.info(result)

        logger.info('Indexing data from Neo4j')
        # progress = tqdm.tqdm(unit="docs", total=number_of_docs)
        successes = 0
        total = 0
        for ok, action in streaming_bulk(
                client=self.es_client,
                index=self.index_name,
                actions=self.get_data(self.cypher_query, self.mapping.keys())
        ):
            # progress.update(1)
            total += 1
            successes += 1 if ok else 0
        logger.info(f"Indexed {successes}/{total} documents")


def main():
    parser = argparse.ArgumentParser(description='Load data from Neo4j to Elasticsearch')
    parser.add_argument('config_file',
                        type=argparse.FileType('r'),
                        help='Configuration file, example is in config/es_loader.example.yml',
                        nargs='?')
    args = parser.parse_args()

    config = yaml.safe_load(args.config_file)['Config']

    neo4j_driver = GraphDatabase.driver(
        config['neo4j_uri'],
        auth=(config['neo4j_user'], config['neo4j_password']),
        encrypted=False
    )

    loader = ESLoader(
        es_host=config['es_host'],
        neo4j_driver=neo4j_driver,
        index_name=config['index_name'],
        mapping=config['mapping'],
        cypher_query=config['cypher_query']
    )
    loader.load()

if __name__ == '__main__':
    main()
