import os.path
import sys
import unittest
from datetime import datetime

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
        actual = self.t.transform_valuedomain(fixture.valuedomain_without_codelist)
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

    def test_represented_variables_different_start_dates(self):
        actual = self.t.transform_represented_variables(fixture.valuedomain_with_codelist_different_start_dates)
        expected = fixture.expected_valuedomain_with_codelist_different_start_dates

        self.assert_values_from_value_domain(expected, actual, 0)
        self.assert_values_from_value_domain(expected, actual, 1)
        self.assert_values_from_value_domain(expected, actual, 2)
        self.assert_values_from_value_domain(expected, actual, 3)

    def assert_values_from_value_domain(self, expected: dict, actual: dict, indeks: int):
        self.assertEqual(expected[indeks]['description'], actual[indeks]['description'])
        self.assertFalse('unitOfMeasure' in expected[indeks].keys())
        self.assertEqual(expected[indeks]['validPeriod'], actual[indeks]['validPeriod'])

    def test_select_code_item_on_start(self):
        code_item = {
            "code": "CAT",
            "categoryTitle": [
                {"languageCode": "no", "value": "Katt"},
                {"languageCode": "en", "value": "Cat"}
            ],
            "validityPeriodStart": "2007-01-01"
        }
        code_list_out = []
        time_period = [datetime(2008, 10, 1, 0, 0), datetime(2009, 12, 31, 0, 0)]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(code_list_out[0], {'category': 'Katt', 'code': 'CAT'})

    def test_do_not_select_code_item_on_start(self):
        code_item = {
            "code": "CAT",
            "categoryTitle": [
                {"languageCode": "no", "value": "Katt"},
                {"languageCode": "en", "value": "Cat"}
            ],
            "validityPeriodStart": "2008-12-01"
        }
        code_list_out = []
        time_period = [datetime(2008, 10, 1, 0, 0), datetime(2009, 12, 31, 0, 0)]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(len(code_list_out), 0)

    def test_select_code_item_on_start_and_stop(self):
        code_item = {
            "code": "CAT",
            "categoryTitle": [
                {"languageCode": "no", "value": "Katt"},
                {"languageCode": "en", "value": "Cat"}
            ],
            "validityPeriodStart": "2008-01-01",
            "validityPeriodStop": "2010-12-31"
        }
        code_list_out = []
        time_period = [datetime(2008, 10, 1, 0, 0), datetime(2009, 12, 31, 0, 0)]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(code_list_out[0], {'category': 'Katt', 'code': 'CAT'})

    def test_do_not_select_code_item_on_stop(self):
        code_item = {
            "code": "CAT",
            "categoryTitle": [
                {"languageCode": "no", "value": "Katt"},
                {"languageCode": "en", "value": "Cat"}
            ],
            "validityPeriodStart": "2007-01-01",
            "validityPeriodStop": "2009-01-01"
        }
        code_list_out = []
        time_period = [datetime(2008, 10, 1, 0, 0), datetime(2009, 12, 31, 0, 0)]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(len(code_list_out), 0)

    def test_select_code_item_when_time_period_has_start_only(self):
        code_item = {
            "code": "CAT",
            "categoryTitle": [
                {"languageCode": "no", "value": "Katt"},
                {"languageCode": "en", "value": "Cat"}
            ],
            "validityPeriodStart": "2008-01-01",
            "validityPeriodStop": "2010-12-31"
        }
        code_list_out = []
        time_period = [datetime(2008, 10, 1, 0, 0), None]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(code_list_out[0], {'category': 'Katt', 'code': 'CAT'})

    def test_do_not_select_code_item_when_time_period_has_start_only(self):
        code_item = {
            "code": "CAT",
            "categoryTitle": [
                {"languageCode": "no", "value": "Katt"},
                {"languageCode": "en", "value": "Cat"}
            ],
            "validityPeriodStart": "2009-01-01",
            "validityPeriodStop": "2010-12-31"
        }
        code_list_out = []
        time_period = [datetime(2008, 10, 1, 0, 0), None]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(len(code_list_out), 0)

    def test_calculate_description_from_value_domain(self):
        actual = self.t.calculate_description_from_value_domain(fixture.valuedomain_with_codelist_different_start_dates)
        expected = 'Type kjæledyr vi selger i butikken'
        self.assertEqual(expected, actual)

    def test_calculate_valid_period_start_stop(self):
        actual = self.t.calculate_valid_period([self.t.to_date('2008-10-01'), self.t.to_date('2009-12-31')])
        expected = {
            "start": self.t.to_date('2008-10-01'),
            "stop": self.t.to_date('2009-12-31')
        }
        self.assertEqual(expected, actual)

    def test_calculate_valid_period_start_only(self):
        actual = self.t.calculate_valid_period([self.t.to_date('2008-10-01')])
        expected = {
            "start": self.t.to_date('2008-10-01')
        }
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    print("Paths used:")
    for place in sys.path:
        print(place)
    unittest.main()
