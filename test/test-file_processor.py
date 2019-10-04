import unittest
from utils import *
import json
from raw_file_processor import *


class TestLambda(unittest.TestCase):
    def setUp(self) -> None:
        with open('data/lambda/event1.json') as inf:
            self.event = json.load(inf)

    def test_join_path(self):
        processor = FileProcessor('')
        self.assertEqual(processor.join_path('abd/def', 'ghi.zip'), 'abd/def/ghi.zip')
        self.assertEqual(processor.join_path('abd/def/', 'ghi.zip'), 'abd/def/ghi.zip')
        self.assertEqual(processor.join_path('abd/def//', '//ghi.zip'), 'abd/def/ghi.zip')
        self.assertEqual(processor.join_path('http://abd/def//', '//ghi.zip//'), 'http://abd/def/ghi.zip//')



    def test_lambda(self):
        processor = FileProcessor('')
        self.assertTrue(processor.handler(self.event))