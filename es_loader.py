#!/user/bin/env python3
import argparse

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

    def load(self, index_name, mapping, cypher_query):
        logger.info(f'Deleting old index "{index_name}"')
        result = self.delete_index(index_name)
        logger.info(result)

        logger.info(f'Creating index "{index_name}"')
        result = self.create_index(index_name, mapping)
        logger.info(result)

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
        loader.load(index['index_name'], index['mapping'], index['cypher_query'])


if __name__ == '__main__':
    main()
