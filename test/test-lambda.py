import unittest
from utils import *
import json
from raw_file_processor import *


class TestLambda(unittest.TestCase):
    def setUp(self) -> None:
        with open('data/lambda/event1.json') as inf:
            self.event = json.load(inf)

    def testLambda(self):
        self.assertIsNone(handler(self.event, None))