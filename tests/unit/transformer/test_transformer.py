import os.path
import sys
import unittest
from datetime import datetime

if __name__ == '__main__':
    # Only munge path if invoked as a script. Testrunners should have setup
    # the paths already
    print("test_transformer.py __main__ called")
    sys.path.insert(0, os.path.abspath(os.path.join(os.pardir, os.pardir, os.pardir)))

from tests.unit.transformer import transformer_fixtures as fixture
from transformer import Transformer


def create_list_of_codes(code_list: list) -> list:
    my_list = []
    for i in code_list:
        my_list.append(i.get('code'))
    my_list.sort()
    return my_list


class TestTransformer(unittest.TestCase):
    t = Transformer()

    description = 'Dette er description fra entiteten som eier valuedomain'

    def test_name_title_description(self):
        """ Common test for UnitType and SubjectField """
        actual = self.t.transform_name_title_description(fixture.name_title_description)
        self.assertEqual(fixture.expected_name_title_description, actual)

    def test_dataset(self):
        actual = self.t.transform_dataset(fixture.dataset)
        expected = fixture.expected_dataset

        self.assertEqual(expected, actual)

    def test_identifier(self):
        actual = self.t.transform_identifier(fixture.dataset['identifier'], '2016-01-01', '2019-12-31')[0]
        expected = fixture.expected_identifier

        self.assertEqual(expected['name'], actual['name'])
        self.assertEqual(expected['label'], actual['label'])
        self.assertEqual(expected['dataType'], actual['dataType'])
        self.assertEqual(expected['representedVariables'][0]['validPeriod'],
                         actual['representedVariables'][0]['validPeriod'])
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

    def test_days_since_epoch(self):
        self.assertEqual(10957, self.t.days_since_epoch('2000-01-01'))

    def test_calculate_days_since_epoch(self):
        days = ['2016-01-01', '2019-12-31', '2010-01-01', '2008-10-01', '2009-12-31', '2011-05-31',
                '2011-06-01', '2011-12-31', '1900-01-01']
        for day in days:
            print(day, ' --> ', self.t.days_since_epoch(day))

    def test_time_periods(self):
        actual = self.t.calculate_time_periods([
            self.t.days_since_epoch("2009-01-01"), self.t.days_since_epoch("2000-01-01"),
            self.t.days_since_epoch("2012-01-01"), self.t.days_since_epoch("2003-01-01")])

        expected = [
            [self.t.days_since_epoch("2000-01-01"), self.t.days_since_epoch("2002-12-31")],
            [self.t.days_since_epoch("2003-01-01"), self.t.days_since_epoch("2008-12-31")],
            [self.t.days_since_epoch("2009-01-01"), self.t.days_since_epoch("2011-12-31")],
            [self.t.days_since_epoch("2012-01-01"), None]
        ]
        self.assertEqual(expected, actual)

    def test_value_domain_with_codelist_same_start_date(self):
        actual = self.t.transform_value_domain_with_codelist(fixture.valuedomain_with_codelist_same_start_date,
                                                             self.description)
        expected = fixture.expected_valuedomain_with_codelist_same_start_date

        self.assert_values_from_value_domain(expected, actual, 0)
        self.assertEqual(len(actual[0]['valueDomain']['codeList']), 3)
        self.assertFalse('stop' in actual[0]['validPeriod'].keys())
        self.assertEqual(create_list_of_codes(actual[0]['valueDomain']['codeList']), ['CAT', 'DOG', 'FISH'])

    def test_value_domain_with_codelist_different_start_dates(self):
        actual = self.t.transform_value_domain_with_codelist(fixture.valuedomain_with_codelist_different_start_dates,
                                                             self.description)
        expected = fixture.expected_valuedomain_with_codelist_different_start_dates

        self.assert_values_from_value_domain(expected, actual, 0)
        self.assert_values_from_value_domain(expected, actual, 1)
        self.assert_values_from_value_domain(expected, actual, 2)
        self.assert_values_from_value_domain(expected, actual, 3)

        self.assertEqual(len(actual[0]['valueDomain']['codeList']), 1)
        self.assertEqual(actual[0]['valueDomain']['codeList'][0]['code'], 'FISH')

        self.assertEqual(len(actual[1]['valueDomain']['codeList']), 3)
        self.assertEqual(create_list_of_codes(actual[1]['valueDomain']['codeList']),
                         ['BIRD', 'CAT', 'FISH'])

        self.assertEqual(len(actual[2]['valueDomain']['codeList']), 4)
        self.assertEqual(create_list_of_codes(actual[2]['valueDomain']['codeList']),
                         ['BIRD', 'CAT', 'FISH', 'RABBIT'])

        self.assertEqual(len(actual[3]['valueDomain']['codeList']), 5)
        self.assertEqual(create_list_of_codes(actual[3]['valueDomain']['codeList']),
                         ['BIRD', 'CAT', 'DOG', 'FISH', 'RABBIT'])

    def test_value_domain_with_codelist_different_start_and_stop_dates(self):
        actual = self.t.transform_value_domain_with_codelist(
            fixture.valuedomain_with_codelist_different_start_and_stop_dates, self.description)
        expected = fixture.expected_valuedomain_with_codelist_different_start_and_stop_dates

        self.assert_values_from_value_domain(expected, actual, 0)
        self.assert_values_from_value_domain(expected, actual, 1)
        self.assert_values_from_value_domain(expected, actual, 2)
        self.assert_values_from_value_domain(expected, actual, 3)
        self.assert_values_from_value_domain(expected, actual, 4)

        self.assertEqual(len(actual[0]['valueDomain']['codeList']), 1)
        self.assertEqual(actual[0]['valueDomain']['codeList'][0]['code'], 'FISH')

        self.assertEqual(len(actual[1]['valueDomain']['codeList']), 3)
        self.assertEqual(create_list_of_codes(actual[1]['valueDomain']['codeList']), ['BIRD', 'CAT', 'FISH'])

        self.assertEqual(len(actual[2]['valueDomain']['codeList']), 2)
        self.assertEqual(create_list_of_codes(actual[2]['valueDomain']['codeList']), ['CAT', 'FISH'])

        self.assertEqual(len(actual[3]['valueDomain']['codeList']), 3)
        self.assertEqual(create_list_of_codes(actual[3]['valueDomain']['codeList']), ['CAT', 'FISH', 'RABBIT'])

        self.assertEqual(len(actual[4]['valueDomain']['codeList']), 4)
        self.assertEqual(create_list_of_codes(actual[4]['valueDomain']['codeList']), ['CAT', 'DOG', 'FISH', 'RABBIT'])

    def assert_values_from_value_domain(self, expected: dict, actual: dict, indeks: int):
        self.assertEqual(expected[indeks]['description'], actual[indeks]['description'])
        self.assertFalse('unitOfMeasure' in actual[indeks].keys())
        self.assertEqual(expected[indeks]['validPeriod'], actual[indeks]['validPeriod'])
        self.assertTrue('missingValues' in actual[indeks]['valueDomain'].keys())

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
        time_period = [self.t.days_since_epoch('2008-10-01'), self.t.days_since_epoch('2009-12-31')]

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
        time_period = [self.t.days_since_epoch('2008-10-01'), self.t.days_since_epoch('2009-12-31')]

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
        time_period = [self.t.days_since_epoch('2008-10-01'), self.t.days_since_epoch('2009-12-31')]

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
        time_period = [self.t.days_since_epoch('2008-10-01'), self.t.days_since_epoch('2009-12-31')]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(len(code_list_out), 0)

    def test_select_code_item_when_time_period_has_start_only(self):
        code_item = {
            "code": "CAT",
            "categoryTitle": [
                {"languageCode": "no", "value": "Katt"},
                {"languageCode": "en", "value": "Cat"}
            ],
            "validityPeriodStart": "2008-01-01"
        }
        code_list_out = []
        time_period = [self.t.days_since_epoch('2008-10-01'), None]

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
        time_period = [self.t.days_since_epoch('2008-10-01'), None]

        self.t.select_code_item(code_item, code_list_out, time_period)
        self.assertEqual(len(code_list_out), 0)

    def test_calculate_description_from_value_domain(self):
        actual = self.t.create_description_from_value_domain(fixture.valuedomain_with_codelist_different_start_dates)
        expected = 'Type kjæledyr vi selger i butikken'
        self.assertEqual(expected, actual)

    def test_calculate_valid_period_start_stop(self):
        actual = self.t.calculate_valid_period(["dummyStart", "dummyStop"])
        expected = {
            "start": "dummyStart",
            "stop": "dummyStop"
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
