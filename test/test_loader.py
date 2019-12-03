import unittest
import os
from utils import get_logger, removeTrailingSlash, UUID
from data_loader import DataLoader
from icdc_schema import ICDC_Schema
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
        self.loader = DataLoader(self.driver, self.schema)
        self.file_list = [
            "data/Dataset/COP-program.txt",
            "data/Dataset/NCATS-COP01-case.txt",
            "data/Dataset/NCATS-COP01-diagnosis.txt",
            "data/Dataset/NCATS-COP01_cohort_file.txt",
            "data/Dataset/NCATS-COP01_study_file.txt"
        ]

    def test_remove_traling_slash(self):
        self.assertEqual('abc', removeTrailingSlash('abc/'))
        self.assertEqual('abc', removeTrailingSlash('abc'))
        self.assertEqual('abc', removeTrailingSlash('abc//'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78/'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78//'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78////'))

    def test_loader_construction(self):
        self.assertRaises(Exception, DataLoader, None, None, None)
        self.assertRaises(Exception, DataLoader, self.driver, None, None)
        self.assertRaises(Exception, DataLoader, self.driver, self.schema , None)
        self.assertRaises(Exception, DataLoader, self.driver, self.schema , ['a', 'b'])
        self.assertIsInstance(self.loader, DataLoader)

    def test_validate_parents_exist_in_file(self):
        load_result = self.loader.load(self.file_list, True, False, 'upsert', False, 1)
        self.assertIsInstance(load_result, dict, msg='Load data failed!')
        result = self.loader.validate_parents_exist_in_file('data/pathology-reports-failure.txt', 100)
        self.assertFalse(result)
        result = self.loader.validate_parents_exist_in_file('data/pathology-reports-success.txt', 100)
        self.assertTrue(result)

    def test_duplicated_ids(self):
        self.assertTrue(self.loader.validate_file('data/Dataset/NCATS-COP01-case.txt', 10))
        self.assertFalse(self.loader.validate_file('data/NCATS01-case-dup.txt', 10))

    def test_get_signature(self):
        self.assertEqual(self.loader.get_signature({}), '{  }')
        self.assertEqual(self.loader.get_signature({'key1': 'value1'}), '{ key1: value1 }')
        self.assertEqual(self.loader.get_signature({'key1': 'value1', 'key2': 'value2'}), '{ key1: value1, key2: value2 }')

    def test_cleanup_node(self):
        #Test UUIDs
        self.assertRaises(Exception, self.loader.prepare_node, {})
        self.assertDictEqual(self.loader.prepare_node({'type': 'case', 'case_id': '123', ' key1 ': ' value1  '}),
                             {'key1': 'value1', 'type': 'case', 'case_id': '123', 'uuid': 'f0cf40a7-3cdb-51fe-a596-e29e40123f56'})
        self.assertDictEqual(self.loader.prepare_node({'type': 'file', 'uuid': '123', ' key1 ': ' value1  '}),
                             {'key1': 'value1', 'type': 'file', 'uuid': '123'})

        # Test parent ids
        obj = self.loader.prepare_node({'type': 'case', 'cohort.cohort_id': 'abc132'})
        self.assertEqual(obj['cohort_id'], 'abc132')
        obj = self.loader.prepare_node({'type': 'case', 'cohort.cohort_id': 'abc132', 'cohort_id': 'def333'})
        self.assertEqual(obj['cohort_id'], 'def333')
        self.assertEqual(obj['cohort_cohort_id'], 'abc132')
        self.assertEqual(len(obj[UUID]), 36)

        # Test Boolean values
        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': 'abc132'})
        self.assertIsNone(obj['ecg'])
        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': 'yes'})
        self.assertEqual(obj['ecg'], True)
        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': 'YeS'})
        self.assertEqual(obj['ecg'], True)
        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': 'YeS13'})
        self.assertEqual(obj['ecg'], True)

        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': 'no'})
        self.assertEqual(obj['ecg'], False)
        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': 'No'})
        self.assertEqual(obj['ecg'], False)
        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': ' No33 '})
        self.assertEqual(obj['ecg'], False)
        obj = self.loader.prepare_node({'type': 'vital_signs', 'ecg': ' Normal '})
        self.assertEqual(obj['ecg'], False)

        # Test Int values
        obj = self.loader.prepare_node({'type': 'physical_exam', 'day_in_cycle': ' Normal '})
        self.assertEqual(obj['day_in_cycle'], None)
        obj = self.loader.prepare_node({'type': 'physical_exam', 'day_in_cycle': ' 13 '})
        self.assertEqual(obj['day_in_cycle'], 13)
        self.assertNotEqual(obj['day_in_cycle'], '13')
        obj = self.loader.prepare_node({'type': 'physical_exam', 'day_in_cycle': ' 12 Normal '})
        self.assertEqual(obj['day_in_cycle'], None)

        #Test Float values
        obj = self.loader.prepare_node({'type': 'file', 'file_size': ' Normal '})
        self.assertEqual(obj['file_size'], None)
        obj = self.loader.prepare_node({'type': 'file', 'file_size': ' 1.5 Normal '})
        self.assertEqual(obj['file_size'], None)
        obj = self.loader.prepare_node({'type': 'file', 'file_size': ' 1.5 '})
        self.assertEqual(obj['file_size'], 1.5)
        obj = self.loader.prepare_node({'type': 'file', 'file_size': ' 15 '})
        self.assertEqual(obj['file_size'], 15)


if __name__ == '__main__':
    unittest.main()