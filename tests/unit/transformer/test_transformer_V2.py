import os.path
import sys
import unittest
import json

#import pathlib

from transformer.transformer_V2 import Transformer
from pathlib import Path

if __name__ == '__main__':
    # Only munge path if invoked as a script. Testrunners should have setup
    # the paths already
    print("test_transformer.py __main__ called")
    sys.path.insert(0, os.path.abspath(os.path.join(os.pardir, os.pardir, os.pardir)))


class TestTransformer(unittest.TestCase):

    transformer = Transformer()

    # def test_paths(self):
    #     print (Path.home())
    #     print (Path.cwd())
    #     print (Path(__file__))
    #     print(Path.cwd().parent)
    #     print(Path(__file__).parent.parent)
    #     print (Path('../../resources/transformer/KREFTREG_DS_described.json').resolve())
    #     print (Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_described.json'))

    def test_dataset_with_enumerated_valuedomain(self):
        dataset, expected = self.load_json_files('KREFTREG_DS_enumerated.json', 'KREFTREG_DS_enumerated_expected.json')
        actual = self.transformer.transform_dataset(dataset)
        print(json.dumps(actual, indent=1))
        self.assertEqual(expected, actual)

    def test_dataset_with_described_valuedomain(self):
        # json_path = Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_described.json')
        # expected_json_path = Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_described_expected.json')
        #
        # with open(json_path.resolve()) as json_file:
        #     dataset = json.load(json_file)
        #
        # with open(expected_json_path.resolve()) as expected_json_file:
        #     expected = json.load(expected_json_file)

        dataset, expected = self.load_json_files('KREFTREG_DS_described.json', 'KREFTREG_DS_described_expected.json')
        actual = self.transformer.transform_dataset(dataset)
        print(json.dumps(actual, indent=1))
        self.assertEqual(expected, actual)

    def test_dataset_with_no_attributes(self):
        dataset, expected = self.load_json_files('KREFTREG_DS_no_attributes.json', 'KREFTREG_DS_no_attributes_expected.json')
        # json_path = Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_no_attributes.json')
        # expected_json_path = Path(__file__).parent.parent.parent.joinpath('resources', 'transformer', 'KREFTREG_DS_no_attributes_expected.json')
        #
        # with open(json_path.resolve()) as json_file:
        #     dataset = json.load(json_file)
        #
        # with open(expected_json_path.resolve()) as expected_json_file:
        #     expected = json.load(expected_json_file)

        actual = self.transformer.transform_dataset(dataset)
        print(json.dumps(actual, indent=1))
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
