import unittest
from utils import *
from icdc_schema import *


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.schema = ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml'])

    def test_schema_construction(self):
        self.assertRaises(Exception, ICDC_Schema, None)
        self.assertRaises(Exception, ICDC_Schema, ['a', 'b'])
        schema = ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml'])
        self.assertIsInstance(schema, ICDC_Schema)
        self.assertEqual(28, schema.node_count())
        self.assertEqual(42, schema.relationship_count())

    def test_default_value(self):
        self.assertIsNone(self.schema.get_default_value('node_does_not_exit', 'unit_does_not_exist'))
        self.assertIsNone(self.schema.get_default_value('adverse_event', 'unit_does_not_exist'))
        self.assertIsNone(self.schema.get_default_value('cycle', 'cycle_number'))
        self.assertEqual('mg/kg', self.schema.get_default_value('adverse_event', 'ae_dose_unit'))
        self.assertEqual('days', self.schema.get_default_value('agent_administration', 'medication_duration_unit'))

    def test_default_unit(self):
        self.assertIsNone(self.schema.get_default_unit('node_does_not_exit', 'unit_does_not_exist'))
        self.assertIsNone(self.schema.get_default_unit('adverse_event', 'unit_does_not_exist'))
        self.assertIsNone(self.schema.get_default_unit('cycle', 'cycle_number'))
        self.assertEqual('mg/kg', self.schema.get_default_unit('adverse_event', 'ae_dose'))
        self.assertEqual('days', self.schema.get_default_unit('agent_administration', 'medication_duration'))


if __name__ == '__main__':
    unittest.main()