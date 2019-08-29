import unittest
from utils import *
from icdc_schema import *


class TestSchema(unittest.TestCase):
    def test_schema_construction(self):
        self.assertRaises(Exception, ICDC_Schema, None)
        self.assertRaises(Exception, ICDC_Schema, ['a', 'b'])
        schema = ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml'])
        self.assertIsInstance(schema, ICDC_Schema)
        self.assertEqual(29, schema.node_count())
        self.assertEqual(37, schema.relationship_count())


if __name__ == '__main__':
    unittest.main()