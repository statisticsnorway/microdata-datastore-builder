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

        self.assertEqual(expected['representedVariables'][0]['validPeriod']['start'],
                         actual['representedVariables'][0]['validPeriod']['start'])
        self.assertEqual(expected['representedVariables'][0]['validPeriod']['stop'],
                         actual['representedVariables'][0]['validPeriod']['stop'])
        self.assertEqual(expected['representedVariables'][0]['description'],
                         actual['representedVariables'][0]['description'])
        self.assertEqual(expected['representedVariables'][0]['valueDomain']['description'],
                         actual['representedVariables'][0]['valueDomain']['description'])
        self.assertEqual(expected['representedVariables'][0]['valueDomain']['unitOfMeasure'],
                         actual['representedVariables'][0]['valueDomain']['unitOfMeasure'])

        self.assertEqual(expected['keyType'], actual['keyType'])
        self.assertEqual(expected['format'], actual['format'])
        self.assertEqual(expected['variableRole'], actual['variableRole'])

        self.assertEqual(f.expected_identifier, actual)

    def test_days_since_epoch(self):
        actual = self.t.days_since_epoch('2000-01-01')
        expected = 10957
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
