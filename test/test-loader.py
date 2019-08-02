import unittest
from loader import *

class TestLoader(unittest.TestCase):
    def test_remove_traling_slash(self):
        self.assertEqual('abc', removeTrailingSlash('abc/'))
        self.assertEqual('abc', removeTrailingSlash('abc'))
        self.assertEqual('abc', removeTrailingSlash('abc//'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78/'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78//'))
        self.assertEqual('bolt://12.34.56.78', removeTrailingSlash('bolt://12.34.56.78////'))

if __name__ == '__main__':
    unittest.main()