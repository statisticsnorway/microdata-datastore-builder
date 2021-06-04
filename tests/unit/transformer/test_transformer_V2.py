import os
import unittest
import json

from transformer.transformer_V2 import Transformer


class TestTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = Transformer()

    def test_dataset_with_enumerated_valuedomain(self):
        with open(os.path.abspath("../../resources/transformer/KREFTREG_DS_enumerated.json")) as json_file:
            dataset = json.load(json_file)
        with open(os.path.abspath("../../resources/transformer/KREFTREG_DS_enumerated_expected.json")) as expected_json_file:
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
