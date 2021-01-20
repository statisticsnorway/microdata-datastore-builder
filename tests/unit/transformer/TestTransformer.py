import unittest
import transformer
from tests.unit.transformer import TransformerFixtures as fixtures


class TestTransformer(unittest.TestCase):

    t = transformer.Transformer()

    def test_unit_type(self):
        actual = self.t.transform_unit_type(fixtures.unit_type)
        self.assertEqual(fixtures.expected_unit_type, actual)


if __name__ == '__main__':
    unittest.main()
