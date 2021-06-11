import json
from pathlib import Path


class Updater:

    def __init__(self):
        pass

    @staticmethod
    def update_metadata_all(transformed_dataset_path: Path, metadata_all_file: Path) -> None:
        dataset = json.loads(transformed_dataset_path.read_text())
        metadata_all_json = json.loads(metadata_all_file.read_text())

        found_existing = False
        for i, data_structure in enumerate(metadata_all_json['dataStructures']):
            if data_structure['name'] == dataset['name']:
                metadata_all_json['dataStructures'][i] = dataset
                found_existing = True

        if not found_existing:
            metadata_all_json['dataStructures'].append(dataset)

        metadata_all_file.write_text(json.dumps(metadata_all_json, sort_keys=False, indent=4, ensure_ascii=False))
