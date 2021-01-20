import unittest
import transformer
from tests.unit.transformer import TransformerFixtures as fixtures


class TestTransformer(unittest.TestCase):

    t = transformer.Transformer()

    def test_name_title_description(self):
        """ Common test for UnitType and SubjectField """
        actual = self.t.transform_name_title_description(fixtures.name_title_description)
        self.assertEqual(fixtures.expected_name_title_description, actual)


if __name__ == '__main__':
    unittest.main()
