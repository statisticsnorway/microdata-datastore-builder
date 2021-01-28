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


def update_metadata_all_file(dataset: dict, source_path: Path) -> None:
    metadata_all_json = json.loads(source_path.read_text())

    found_existing = False
    for i, data_structure in enumerate(metadata_all_json['dataStructures']):
        if data_structure['name'] == dataset['name']:
            metadata_all_json['dataStructures'][i] = dataset
            found_existing = True

    if not found_existing:
        metadata_all_json['dataStructures'].append(dataset)

    source_path.write_text(json.dumps(metadata_all_json, sort_keys=False, indent=4, ensure_ascii=False))

