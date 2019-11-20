import unittest
from utils import *
from loader import *
from data_loader import *
from icdc_schema import *
import os
from neo4j import GraphDatabase


class TestLoader(unittest.TestCase):
    def setUp(self):
        uri = 'bolt://localhost:7687'
        user = 'neo4j'
        password = os.environ['NEO_PASSWORD']

        self.driver = GraphDatabase.driver(uri, auth = (user, password))
        self.data_folder = 'data/COTC007B'
        self.schema = ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml'])
        self.log = get_logger('Test Loader')
        self.file_list = [
            "data/COTC007B/COP-program.txt",
            "data/COTC007B/COTC007B-case.txt",
            "data/COTC007B/COTC007B-cohort.txt",
            "data/COTC007B/COTC007B-cycle.txt",
            "data/COTC007B/COTC007B-demographic.txt",
            "data/COTC007B/COTC007B-diagnostic.txt",
            "data/COTC007B/COTC007B-enrollment.txt",
            "data/COTC007B/COTC007B-extent_of_disease.txt",
            "data/COTC007B/COTC007B-physical_exam.txt",
            "data/COTC007B/COTC007B-principal_investigator.txt",
            "data/COTC007B/COTC007B-prior_surgery.txt",
            "data/COTC007B/COTC007B-study.txt",
            "data/COTC007B/COTC007B-study_arm.txt",
            "data/COTC007B/COTC007B-vital_signs.txt",
            "data/NCATS/NCATS-COP01-blood_samples.txt",
            "data/NCATS/NCATS-COP01-case.txt",
            "data/NCATS/NCATS-COP01-demographic.txt",
            "data/NCATS/NCATS-COP01-diagnosis.txt",
            "data/NCATS/NCATS-COP01-enrollment.txt",
            "data/NCATS/NCATS-COP01-normal_samples.txt",
            "data/NCATS/NCATS-COP01-tumor_samples.txt",
            "data/NCATS/NCATS-COP01_20170228-GSL-079A-PE-Breen-NCATS-MEL-Rep1-Lane3.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep1-Lane1.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep1-Lane2.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep2-Lane1.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep3-Lane1.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep2-Lane2.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep2-Lane3.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep3-Lane2.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep3-Lane3.tar-file_neo4j.txt",
            "data/NCATS/NCATS-COP01_cohort_file.txt",
            "data/NCATS/NCATS-COP01_path_report_file_neo4j.txt",
            "data/NCATS/NCATS-COP01_study_file.txt"
        ]

    def test_load(self):
        with self.driver.session() as session:
            cleanup_db = 'MATCH (n) DETACH DELETE n'
            result = session.run(cleanup_db)
            self.log.info('{} nodes deleted!'.format(result.summary().counters.nodes_deleted))
            self.log.info('{} relationships deleted!'.format(result.summary().counters.relationships_deleted))
        loader = DataLoader(self.driver, self.schema)
        load_result = loader.load(self.file_list, True, False, 1)
        self.assertIsInstance(load_result, dict, msg='Load data failed!')
        self.assertEqual(1832, load_result[NODES_CREATED])
        self.assertEqual(1974, load_result[RELATIONSHIP_CREATED])


if __name__ == '__main__':
    unittest.main()