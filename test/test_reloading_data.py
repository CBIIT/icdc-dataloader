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
            "data/COTC007B/COTC007B-0206-0208-disease_extent.txt",
            "data/COTC007B/COTC007B-0206-0208-physical_exam.txt",
            "data/COTC007B/COTC007B-0206-0208-vital_signs.txt",
            "data/COTC007B/COTC007B-0402-disease_extent.txt",
            "data/COTC007B/COTC007B-0402-physical_exam.txt",
            "data/COTC007B/COTC007B-0402-vital_signs.txt",
            "data/COTC007B/COTC007B-cycle.txt",
            "data/COTC007B/COTC007B_case.txt",
            "data/COTC007B/COTC007B_cohort.txt",
            "data/COTC007B/COTC007B_demographic.txt",
            "data/COTC007B/COTC007B_diagnosis.txt",
            "data/COTC007B/COTC007B_enrollment.txt",
            "data/COTC007B/COTC007B_prior_surgery.txt",
            "data/COTC007B/COTC007B_study.txt",
            "data/COTC007B/COTC007B_study_arm.txt",
            "data/COTC007B/COTC_program.txt",
            "data/NCATS/NCATS01-arm.txt",
            "data/NCATS/NCATS01-blood_samples.txt",
            "data/NCATS/Pathology-Report-Mapping-File_neo4j.txt",
            "data/NCATS/Example_Mapping_file_for_30_sequence_files_to_ICDC_Samples_from_Melanoma_subjects_neo4j.txt",
            "data/NCATS/NCATS01-case.txt",
            "data/NCATS/NCATS01-program.txt",
            "data/NCATS/NCATS01-cohort.txt",
            "data/NCATS/NCATS01-demographic.txt",
            "data/NCATS/NCATS01-diagnosis.txt",
            "data/NCATS/NCATS01-enrollment.txt",
            "data/NCATS/NCATS01-normal_samples.txt",
            "data/NCATS/NCATS01-study.txt",
            "data/NCATS/NCATS01-tumor_samples.txt"
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
        self.assertEqual(1458, load_result[NODES_CREATED])
        self.assertEqual(1599, load_result[RELATIONSHIP_CREATED])


if __name__ == '__main__':
    unittest.main()