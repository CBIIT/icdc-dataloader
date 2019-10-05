import unittest
from utils import *
import json
from neo4j import GraphDatabase
from raw_file_processor import *
from icdc_schema import ICDC_Schema


class TestLambda(unittest.TestCase):
    def setUp(self) -> None:
        with open('data/lambda/event1.json') as inf:
            self.event = json.load(inf)
        uri = 'bolt://localhost:7687'
        user = 'neo4j'
        password = os.environ['NEO_PASSWORD']

        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.schema = ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml'])
        self.processor = FileProcessor('', self.driver, self.schema)

    def test_join_path(self):
        self.assertEqual(self.processor.join_path('abd/def', 'ghi.zip'), 'abd/def/ghi.zip')
        self.assertEqual(self.processor.join_path('abd/def/', 'ghi.zip'), 'abd/def/ghi.zip')
        self.assertEqual(self.processor.join_path('abd/def//', '//ghi.zip'), 'abd/def/ghi.zip')
        self.assertEqual(self.processor.join_path('http://abd/def//', '//ghi.zip//'), 'http://abd/def/ghi.zip//')


    def test_lambda(self):
        self.assertTrue(self.processor.handler(self.event))