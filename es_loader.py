#!/user/bin/env python3
import os

from neo4j import GraphDatabase
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from bento.common.utils import get_logger

logger = get_logger('ESLoader')

class ESLoader:
    def __init__(self, es_endpoint, neo4j_driver, index_name, mapping, cypher_query):
        self.neo4j_driver = neo4j_driver
        self.es_client = Elasticsearch(hosts=[es_endpoint])
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

        logger.info('Creating index')
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
    neo4j_uri = 'bolt://127.0.0.1:7687'
    neo4j_user = 'neo4j'
    neo4j_password = os.environ['NEO_PASSWORD']
    es_endpoint = 'localhost'
    index_name = 'dashboard'
    mapping = {
        "program": {"type": "keyword"},
        "study": {"type": "keyword"},
        "diagnosis": {"type": "keyword"},
        "rc_score": {"type": "keyword"},
        "tumor_size": {"type": "keyword"},
        "chemo_regimen": {"type": "keyword"},
        "tumor_grade": {"type": "keyword"},
        "er_status": {"type": "keyword"},
        "pr_status": {"type": "keyword"},
        "endo_therapy": {"type": "keyword"},
        "meno_status": {"type": "keyword"},
        "tissue_type": {"type": "keyword"},
        "composition": {"type": "keyword"},
        "association": {"type": "keyword"},
        "file_type": {"type": "keyword"},
    }

    cypher_query = """
        MATCH (ss)<-[:sf_of_study_subject]-(sf)
        MATCH (ss)<-[:diagnosis_of_study_subject]-(d)<-[:tp_of_diagnosis]-(tp)
        MATCH (ss:study_subject)-[:study_subject_of_study]->(s)-[:study_of_program]->(p)
        MATCH (ss)<-[:demographic_of_study_subject]-(demo)
        MATCH (ss)<-[:sample_of_study_subject]-(samp)
        MATCH (ss)<-[*..2]-(parent)<--(f:file)
        OPTIONAL MATCH (f)-[:file_of_laboratory_procedure]->(lp)
        OPTIONAL MATCH (ss)-[:study_subject_of_study]->(s)-[:study_of_program]->(p)
        OPTIONAL MATCH (ss)<-[:sf_of_study_subject]-(sf)
        OPTIONAL MATCH (ss)<-[:diagnosis_of_study_subject]-(d)
        OPTIONAL MATCH (d)<-[:tp_of_diagnosis]-(tp)
        OPTIONAL MATCH (ss)<-[:demographic_of_study_subject]-(demo)
        RETURN  
            p.program_acronym AS program,
            (s.study_acronym + ': ' + s.study_short_description) AS study,
            ss.disease_subtype AS diagnosis,
            sf.grouped_recurrence_score AS rc_score,
            d.tumor_size_group AS tumor_size,
            tp.chemotherapy_regimen AS chemo_regimen,
            d.tumor_grade AS tumor_grade,
            d.er_status AS er_status,
            d.pr_status AS pr_status,
            tp.endocrine_therapy_type AS endo_therapy,
            demo.menopause_status AS  meno_status,
            samp.tissue_type AS tissue_type,
            samp.composition AS composition,
            head(labels(parent)) AS association,
            f.file_type AS file_type
    """

    neo4j_driver = GraphDatabase.driver(
        neo4j_uri,
        auth=(neo4j_user, neo4j_password),
        encrypted=False
    )

    loader = ESLoader(es_endpoint=es_endpoint, neo4j_driver=neo4j_driver, index_name=index_name, mapping=mapping,
                      cypher_query=cypher_query)
    loader.load()

if __name__ == '__main__':
    main()
