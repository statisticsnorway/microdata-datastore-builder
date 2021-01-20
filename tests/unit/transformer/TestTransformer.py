import unittest
import transformer
from tests.unit.transformer import TransformerFixtures as f


class TestTransformer(unittest.TestCase):
    t = transformer.Transformer()

    def test_name_title_description(self):
        """ Common test for UnitType and SubjectField """
        actual = self.t.transform_name_title_description(f.name_title_description)
        self.assertEqual(f.expected_name_title_description, actual)

    def test_identifier(self):
        actual = self.t.transform_identifier(f.dataset)
        expected = f.expected_identifier

        self.assertEqual(expected['name'], actual['name'])
        self.assertEqual(expected['label'], actual['label'])
        self.assertEqual(expected['dataType'], actual['dataType'])
        self.assertEqual(expected['format'], actual['format'])

        #self.assertEqual(f.expected_identifier, actual)


if __name__ == '__main__':
    unittest.main()
