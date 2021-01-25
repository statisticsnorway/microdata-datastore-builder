import os.path
import sys
import unittest

if __name__ == '__main__':
    # Only munge path if invoked as a script. Testrunners should have setup
    # the paths already
    print("test_transformer.py __main__ called")
    sys.path.insert(0, os.path.abspath(os.path.join(os.pardir, os.pardir, os.pardir)))

from tests.unit.transformer import TransformerFixtures as fixture
from transformer import Transformer


class TestTransformer(unittest.TestCase):
    t = Transformer()

    def test_name_title_description(self):
        """ Common test for UnitType and SubjectField """
        actual = self.t.transform_name_title_description(fixture.name_title_description)
        self.assertEqual(fixture.expected_name_title_description, actual)

    def test_identifier(self):
        actual = self.t.transform_identifier(fixture.dataset)
        expected = fixture.expected_identifier

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

        self.assertEqual(fixture.expected_identifier, actual)

    def test_valuedomain_without_codelist(self):
        actual = self.t.transform_valuedomain(fixture.dataset['identifier'][0]['valueDomain'])
        expected = fixture.expected_valuedomain_without_codelist
        self.assertEqual(expected['description'], actual['description'])
        self.assertEqual(expected['unitOfMeasure'], actual['unitOfMeasure'])

    def test_days_since_epoch(self):
        actual = self.t.days_since_epoch('2000-01-01')
        expected = 10957
        self.assertEqual(expected, actual)

    def test_time_periods(self):
        actual = self.t.calculate_time_periods(["2009-01-01", "2000-01-01", "2012-01-01", "2003-01-01"])
        expected = [
            [self.t.to_date("2000-01-01"), self.t.to_date("2002-12-31")],
            [self.t.to_date("2003-01-01"), self.t.to_date("2008-12-31")],
            [self.t.to_date("2009-01-01"), self.t.to_date("2011-12-31")],
            [self.t.to_date("2012-01-01"), None]
        ]
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    print("Paths used:")
    for place in sys.path:
        print(place)
    unittest.main()
