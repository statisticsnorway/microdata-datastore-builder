import unittest
import transformer
from tests.unit.transformer import TransformerFixtures as fixtures


class TestTransformer(unittest.TestCase):

    def test_transform(self):
        self.assertEqual(4, transformer.Transformer.transform(2))

    def test_unit_type(self):
        actual = transformer.Transformer.transform_unit_type(fixtures.unit_type)
        self.assertEqual(fixtures.expected_unit_type, actual)

if __name__ == '__main__':
    unittest.main()
