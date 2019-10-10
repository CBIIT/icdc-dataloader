import unittest
from utils import *
from loader import *
from data_loader import *
from icdc_schema import *


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
            "data/NCATS/NCATS-Pothology-Report-20190912-104934.txt",
            "data/NCATS/NCATS-Sequence-File-20190912-103744.txt",
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
        self.loader = DataLoader(self.driver, self.schema, self.file_list)

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
        # result = loader.validate_parents_exit_in_file('data/Pathology-Report-Mapping-File.txt', 100)
        result = self.loader.validate_cases_exist_in_file('data/pathology-reports-failure.txt', 100)
        self.assertFalse(result)
        result = self.loader.validate_cases_exist_in_file('data/pathology-reports-success.txt', 100)
        self.assertTrue(result)

    def test_duplicated_ids(self):
        self.assertTrue(self.loader.validate_file('data/NCATS/NCATS01-case.txt', 10))
        self.assertFalse(self.loader.validate_file('data/NCATS01-case-dup.txt', 10))

    def test_get_value_string(self):
        # Test String type
        self.assertIsNone(self.loader.get_value_string('adverse_event', 'adverse_event_description', None))
        self.assertIsNone(self.loader.get_value_string('adverse_event', 'adverse_event_description', []))
        self.assertIsNone(self.loader.get_value_string('adverse_event', 'adverse_event_description', [0]))
        self.assertIsNone(self.loader.get_value_string('adverse_event', 'adverse_event_description', ['0']))
        self.assertIsNone(self.loader.get_value_string('adverse_event', 'adverse_event_description', {}))
        self.assertIsNone(self.loader.get_value_string('adverse_event', 'adverse_event_description', {'a': 'b'}))
        self.assertEqual(self.loader.get_value_string('adverse_event', 'adverse_event_description', ''), '""')
        self.assertEqual(self.loader.get_value_string('adverse_event', 'adverse_event_description', 'abc'), '"abc"')

        # Test Boolean type
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', ''), '""')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', None), '""')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', []), '""')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', {}), '""')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'other value'), '""')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'yes'), 'True')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'Yes'), 'True')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'YeS'), 'True')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'true'), 'True')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'True'), 'True')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'TruE'), 'True')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'false'), 'False')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'False'), 'False')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'fAlsE'), 'False')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'no'), 'False')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'No'), 'False')
        self.assertEqual(self.loader.get_value_string('diagnosis', 'concurrent_disease', 'NO'), 'False')

        # Test Integer value
        self.assertIsNone(self.loader.get_value_string('physical_exam', 'day_in_cycle', ''))
        self.assertIsNone(self.loader.get_value_string('physical_exam', 'day_in_cycle', None))
        self.assertIsNone(self.loader.get_value_string('physical_exam', 'day_in_cycle', 'not a number'))
        self.assertEqual(self.loader.get_value_string('physical_exam', 'day_in_cycle', '3'), 3)

        # Test Float value
        self.assertIsNone(self.loader.get_value_string('prior_therapy', 'total_dose', ''))
        self.assertIsNone(self.loader.get_value_string('prior_therapy', 'total_dose', None))
        self.assertIsNone(self.loader.get_value_string('prior_therapy', 'total_dose', 'not a number'))
        self.assertEqual(self.loader.get_value_string('prior_therapy', 'total_dose', '3'), 3.0)
        self.assertEqual(self.loader.get_value_string('prior_therapy', 'total_dose', '3.0'), 3.0)


if __name__ == '__main__':
    unittest.main()