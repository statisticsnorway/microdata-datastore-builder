import os.path
import sys
import unittest
import json

from datastore_builder.transformer import Transformer

from pathlib import Path

if __name__ == '__main__':
    # Only munge path if invoked as a script. Testrunners should have setup
    # the paths already
    print("test_transformer.py __main__ called")
    sys.path.insert(0, os.path.abspath(os.path.join(os.pardir, os.pardir, os.pardir)))


class TestTransformer(unittest.TestCase):

    transformer = Transformer()

    def test_dataset_with_enumerated_valuedomain(self):
        dataset, expected = self.load_json_files('KREFTREG_DS_enumerated.json', 'KREFTREG_DS_enumerated_expected.json')
        actual = self.transformer.transform_dataset(dataset)
        self.assertEqual(expected, actual)

    def test_dataset_with_described_valuedomain(self):
        dataset, expected = self.load_json_files('KREFTREG_DS_described.json', 'KREFTREG_DS_described_expected.json')
        actual = self.transformer.transform_dataset(dataset)
        self.assertEqual(expected, actual)

    def test_dataset_with_no_attributes(self):
        dataset, expected = self.load_json_files('KREFTREG_DS_no_attributes.json', 'KREFTREG_DS_no_attributes_expected.json')
        actual = self.transformer.transform_dataset(dataset)
        self.assertEqual(expected, actual)

    # This use of Path ensures the paths are found both locally and on Azure pipelines.
    def load_json_files(self, input_file: str, expected_file: str):
        json_path = Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', input_file)
        expected_json_path = Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', expected_file)
        with open(json_path.resolve()) as json_file:
            dataset = json.load(json_file)
        with open(expected_json_path.resolve()) as expected_json_file:
            expected = json.load(expected_json_file)
        return dataset, expected
