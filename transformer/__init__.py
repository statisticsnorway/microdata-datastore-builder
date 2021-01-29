import json
from pathlib import Path

from .transformer import Transformer

"""
Read a json file, transform and write to another file
"""


def transform_to_file(read_from: Path, write_to: Path):
    dataset = json.loads(read_from.read_text())
    transformed_dataset = Transformer.transform_dataset(dataset)
    write_to.write_text(json.dumps(transformed_dataset, sort_keys=False, indent=4, ensure_ascii=False))


def update_metadata_all_file(transformed_dataset_path: Path, source_path: Path) -> None:
    dataset = json.loads(transformed_dataset_path.read_text())
    metadata_all_json = json.loads(source_path.read_text())

    found_existing = False
    for i, data_structure in enumerate(metadata_all_json['dataStructures']):
        if data_structure['name'] == dataset['name']:
            metadata_all_json['dataStructures'][i] = dataset
            found_existing = True

    if not found_existing:
        metadata_all_json['dataStructures'].append(dataset)

    source_path.write_text(json.dumps(metadata_all_json, sort_keys=False, indent=4, ensure_ascii=False))
