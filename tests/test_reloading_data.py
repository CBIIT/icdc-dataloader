import unittest
from common.utils import get_logger, NODES_CREATED, RELATIONSHIP_CREATED, NODES_DELETED, RELATIONSHIP_DELETED
from common.data_loader import DataLoader
from common.icdc_schema import ICDC_Schema
import os
from neo4j import GraphDatabase


class TestLoaderReload(unittest.TestCase):
    def setUp(self):
        uri = 'bolt://localhost:7687'
        user = 'neo4j'
        password = os.environ['NEO_PASSWORD']

        self.driver = GraphDatabase.driver(uri, auth = (user, password))
        self.data_folder = 'data/COTC007B'
        self.schema = ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml'])
        self.log = get_logger('Test Loader')
        self.loader = DataLoader(self.driver, self.schema)
        self.file_list = [
            "data/Dataset/COP-program.txt",
            "data/Dataset/COTC007B-case.txt",
            "data/Dataset/COTC007B-cohort.txt",
            "data/Dataset/COTC007B-cycle.txt",
            "data/Dataset/COTC007B-demographic.txt",
            "data/Dataset/COTC007B-diagnostic.txt",
            "data/Dataset/COTC007B-enrollment.txt",
            "data/Dataset/COTC007B-extent_of_disease.txt",
            "data/Dataset/COTC007B-physical_exam.txt",
            "data/Dataset/COTC007B-principal_investigator.txt",
            "data/Dataset/COTC007B-prior_surgery.txt",
            "data/Dataset/COTC007B-study.txt",
            "data/Dataset/COTC007B-study_arm.txt",
            "data/Dataset/COTC007B-vital_signs.txt",
            "data/Dataset/NCATS-COP01-blood_samples.txt",
            "data/Dataset/NCATS-COP01-case.txt",
            "data/Dataset/NCATS-COP01-demographic.txt",
            "data/Dataset/NCATS-COP01-diagnosis.txt",
            "data/Dataset/NCATS-COP01-enrollment.txt",
            "data/Dataset/NCATS-COP01-normal_samples.txt",
            "data/Dataset/NCATS-COP01-tumor_samples.txt",
            "data/Dataset/NCATS-COP01_20170228-GSL-079A-PE-Breen-NCATS-MEL-Rep1-Lane3.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep1-Lane1.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep1-Lane2.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep2-Lane1.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep3-Lane1.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep2-Lane2.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep2-Lane3.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep3-Lane2.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep3-Lane3.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_cohort_file.txt",
            "data/Dataset/NCATS-COP01_path_report_file_neo4j.txt",
            "data/Dataset/NCATS-COP01_study_file.txt"
        ]
        self.file_list_unique = [
            "data/Dataset/COP-program.txt",
            "data/Dataset/COTC007B-case.txt",
            "data/Dataset/COTC007B-cohort.txt",
            "data/Dataset/COTC007B-cycle.txt",
            "data/Dataset/COTC007B-demographic.txt",
            "data/Dataset/COTC007B-diagnostic.txt",
            "data/Dataset/COTC007B-enrollment.txt",
            "data/Dataset/COTC007B-extent_of_disease.txt",
            "data/Dataset/COTC007B-physical_exam.txt",
            "data/Dataset/COTC007B-principal_investigator.txt",
            "data/Dataset/COTC007B-prior_surgery.txt",
            "data/Dataset/COTC007B-study.txt",
            "data/Dataset/COTC007B-study_arm.txt",
            "data/Dataset/COTC007B-vital_signs_unique.txt",
            "data/Dataset/NCATS-COP01-blood_samples.txt",
            "data/Dataset/NCATS-COP01-case.txt",
            "data/Dataset/NCATS-COP01-demographic.txt",
            "data/Dataset/NCATS-COP01-diagnosis.txt",
            "data/Dataset/NCATS-COP01-enrollment.txt",
            "data/Dataset/NCATS-COP01-normal_samples.txt",
            "data/Dataset/NCATS-COP01-tumor_samples.txt",
            "data/Dataset/NCATS-COP01_20170228-GSL-079A-PE-Breen-NCATS-MEL-Rep1-Lane3.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep1-Lane1.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep1-Lane2.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep2-Lane1.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-076A-Breen-NCATS-MEL-Rep3-Lane1.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep2-Lane2.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep2-Lane3.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep3-Lane2.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_GSL-079A-Breen-NCATS-MEL-Rep3-Lane3.tar-file_neo4j.txt",
            "data/Dataset/NCATS-COP01_cohort_file.txt",
            "data/Dataset/NCATS-COP01_path_report_file_neo4j.txt",
            "data/Dataset/NCATS-COP01_study_file.txt"
        ]


    def test_load_detect_duplicate(self):
        self.assertRaises(Exception, self.loader.load(["data/COTC007B/COTC007B-vital_signs.txt"], True, False, 'new', True, 1))


    def test_reload_with_new_and_delete_cohorts(self):
        load_result = self.loader.load(self.file_list_unique, True, False, 'new', True, 1)
        self.assertIsInstance(load_result, dict, msg='Load data failed!')
        self.assertEqual(1832, load_result[NODES_CREATED])
        self.assertEqual(1974, load_result[RELATIONSHIP_CREATED])
        result = self.loader.load(['data/Dataset/COTC007B-cohort.txt'], True, False, 'delete', False, 1)
        self.assertEqual(result[NODES_DELETED], 18)
        self.assertEqual(result[RELATIONSHIP_DELETED], 101)

    def test_reload_with_new_and_delete_study(self):
        load_result = self.loader.load(self.file_list_unique, True, False, 'new', True, 1)
        self.assertIsInstance(load_result, dict, msg='Load data failed!')
        self.assertEqual(1832, load_result[NODES_CREATED])
        self.assertEqual(1974, load_result[RELATIONSHIP_CREATED])
        result = self.loader.load(['data/Dataset/COTC007B-study.txt'], True, False, 'delete', False, 1)
        self.assertEqual(result[NODES_DELETED], 1118)
        self.assertEqual(result[RELATIONSHIP_DELETED], 1201)

        result = self.loader.load(['data/Dataset/NCATS-COP01_study_file.txt'], True, False, 'delete', False, 1)
        self.assertEqual(result[NODES_DELETED], 713)
        self.assertEqual(result[RELATIONSHIP_DELETED], 773)

    def test_reload_with_new_and_delete_program(self):
        load_result = self.loader.load(self.file_list_unique, True, False, 'new', True, 1)
        self.assertIsInstance(load_result, dict, msg='Load data failed!')
        self.assertEqual(1832, load_result[NODES_CREATED])
        self.assertEqual(1974, load_result[RELATIONSHIP_CREATED])
        result = self.loader.load(['data/Dataset/COP-program.txt'], True, False, 'delete', False, 1)
        self.assertEqual(result[NODES_DELETED], 1832)
        self.assertEqual(result[RELATIONSHIP_DELETED], 1974)


    def test_reload_upsert(self):
        load_result = self.loader.load(self.file_list, True, False, 'upsert', True, 1)
        self.assertIsInstance(load_result, dict, msg='Load data failed!')
        self.assertEqual(1832, load_result[NODES_CREATED])
        self.assertEqual(1974, load_result[RELATIONSHIP_CREATED])



if __name__ == '__main__':
    unittest.main()