import json
from pathlib import Path

from .Transformer import Transformer


def transform_to_file():
    pass


def update_metadata_all_file(source_path: Path, dataset: dict) -> None:
    metadata_all_json = json.loads(source_path.read_text())

    for data_structure in metadata_all_json['dataStructures']:
        if data_structure['name'] == dataset['name']:
            return True
