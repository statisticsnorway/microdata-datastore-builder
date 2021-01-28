import json
from pathlib import Path

from .transformer import Transformer


def transform_to_file():
    pass


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

