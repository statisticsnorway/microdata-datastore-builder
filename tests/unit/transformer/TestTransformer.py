import unittest
import transformer


class TestTransformer(unittest.TestCase):

    def test_transform(self):
        self.assertEqual(4, transformer.Transformer.transform(2))


if __name__ == '__main__':
    unittest.main()
