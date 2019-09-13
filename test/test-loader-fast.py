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
            "data/NCATS/NCATS01-pathology-reports-20190910-170434.txt",
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
        loader = DataLoader(self.driver, self.schema, self.file_list)
        self.assertIsInstance(loader, DataLoader)

    def test_validate_parents_exist_in_file(self):
        loader = DataLoader(self.driver, self.schema, self.file_list)
        # result = loader.validate_parents_exit_in_file('data/Pathology-Report-Mapping-File.txt', 100)
        result = loader.validate_cases_exist_in_file('data/pathology-reports-failure.txt', 100)
        self.assertFalse(result)
        result = loader.validate_cases_exist_in_file('data/pathology-reports-success.txt', 100)
        self.assertTrue(result)

    def test_duplicated_ids(self):
        loader = DataLoader(self.driver, self.schema, self.file_list)
        self.assertTrue(loader.validate_file('data/NCATS/NCATS01-case.txt', 10))
        self.assertFalse(loader.validate_file('data/NCATS01-case-dup.txt', 10))


if __name__ == '__main__':
    unittest.main()