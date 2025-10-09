import unittest
from wmscraper4000 import dummy

class TestScraper(unittest.TestCase):
    def test_dummy(self):
        self.assertEqual(dummy(), "Hello, World!")

if __name__ == '__main__':
    unittest.main()