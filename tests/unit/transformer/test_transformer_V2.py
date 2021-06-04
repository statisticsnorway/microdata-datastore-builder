import os.path
import sys
import unittest
import json

import pathlib

from transformer.transformer_V2 import Transformer
from pathlib import Path

if __name__ == '__main__':
    # Only munge path if invoked as a script. Testrunners should have setup
    # the paths already
    print("test_transformer.py __main__ called")
    sys.path.insert(0, os.path.abspath(os.path.join(os.pardir, os.pardir, os.pardir)))


class TestTransformer(unittest.TestCase):

    # def setUp(self):
    #     self.transformer = Transformer()

    # transformer = None
    # json_file = None
    # json_file_expected = None
    #
    # @classmethod
    # def setUpClass(cls):
    #     cls.transformer = Transformer()
    #     cls.json_file = "tests/resources/transformer/KREFTREG_DS_enumerated.json"
    #     cls.json_file_expected = "tests/resources/transformer/KREFTREG_DS_enumerated_expected.json"

    transformer = Transformer()

    def test_paths(self):
        print (Path.home())
        print (Path.cwd())
        print (Path(__file__))
        print(Path.cwd().parent)
        print(Path(__file__).parent.parent)
        print (Path('../../resources/transformer/KREFTREG_DS_described.json').resolve())
        print (Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_described.json'))

    def test_dataset_with_enumerated_valuedomain(self):
        # json_path = Path('../../resources/transformer/KREFTREG_DS_enumerated.json')
        # json_path = Path.cwd().parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_enumerated.json')
        json_path = Path('microdata-datastore-builder/tests/resources/transformer/KREFTREG_DS_enumerated.json')

        # expected_json_path = Path('../../resources/transformer/KREFTREG_DS_enumerated_expected.json')
        # expected_json_path = Path.cwd().parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_enumerated_expected.json')
        expected_json_path = Path('microdata-datastore-builder/tests/resources/transformer/KREFTREG_DS_enumerated_expected.json')

        with open(json_path.resolve()) as json_file:
            dataset = json.load(json_file)

        with open(expected_json_path.resolve()) as expected_json_file:
            expected = json.load(expected_json_file)

        actual = self.transformer.transform_dataset(dataset)
        print(json.dumps(actual, indent=1))
        self.assertEqual(expected, actual)

    def test_dataset_with_described_valuedomain(self):
        with open(os.path.abspath("../../resources/transformer/KREFTREG_DS_described.json")) as json_file:
            dataset = json.load(json_file)
        with open(os.path.abspath(
                "../../resources/transformer/KREFTREG_DS_described_expected.json")) as expected_json_file:
            expected = json.load(expected_json_file)
        actual = self.transformer.transform_dataset(dataset)
        print(json.dumps(actual, indent=1))
        self.assertEqual(expected, actual)

    def test_dataset_with_no_attributes(self):
        with open(os.path.abspath("../../resources/transformer/KREFTREG_DS_no_attributes.json")) as json_file:
            dataset = json.load(json_file)
        with open(os.path.abspath(
                "../../resources/transformer/KREFTREG_DS_no_attributes_expected.json")) as expected_json_file:
            expected = json.load(expected_json_file)
        actual = self.transformer.transform_dataset(dataset)
        print(json.dumps(actual, indent=1))
        self.assertEqual(expected, actual)
