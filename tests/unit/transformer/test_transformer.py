import os.path
import sys

if __name__ == '__main__':
    # Only munge path if invoked as a script. Testrunners should have setup
    # the paths already
    print("test_transformer.py __main__ called")
    sys.path.insert(0, os.path.abspath(os.path.join(os.pardir, os.pardir, os.pardir)))

import unittest
from transformer import Transformer
from tests.unit.transformer import TransformerFixtures as fixtures


class TestTransformer(unittest.TestCase):
    t = Transformer()

    def test_name_title_description(self):
        """ Common test for UnitType and SubjectField """
        actual = self.t.transform_name_title_description(fixtures.name_title_description)
        self.assertEqual(fixtures.expected_name_title_description, actual)


if __name__ == '__main__':
    print("Paths used:")
    for place in sys.path:
        print(place)
    unittest.main()
