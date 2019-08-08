import unittest
from utils import *
from icdc_schema import *


class TestSchema(unittest.TestCase):
    def test_schema_construction(self):
        self.assertRaises(Exception, ICDC_Schema, None)
        self.assertRaises(Exception, ICDC_Schema, ['a', 'b'])
        self.assertIsInstance(ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml']), ICDC_Schema)

if __name__ == '__main__':
    unittest.main()