import json
from pathlib import Path

from .transformer import Transformer

"""
Read a json file, transform and write to another file
"""


def transform_to_file(read_from: str, write_to:str):
    with open(read_from) as json_file:
        dataset = json.load(json_file)
    transformed_dataset = Transformer.transform_dataset(dataset)
    with open(write_to, 'w') as outfile:
        json.dump(transformed_dataset, outfile, indent=4)


def update_metadata_all_file(source_path: Path, dataset: dict) -> None:
    metadata_all_json = json.loads(source_path.read_text())

    for data_structure in metadata_all_json['dataStructures']:
        if data_structure['name'] == dataset['name']:
            return True
