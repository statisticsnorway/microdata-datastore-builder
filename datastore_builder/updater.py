import json
from pathlib import Path

from common import log_config


class Updater:

    def __init__(self, log_filter):
        self.logger = log_config.get_logger_for_import_pipeline("Updater")
        self.logger.addFilter(log_filter)
        self.logger.info('creating an instance of Updater')

    def update_metadata_all(self, transformed_dataset: str, metadata_all_file: str) -> None:

        dataset = json.loads(Path(transformed_dataset).read_text())
        metadata_all_json = json.loads(Path(metadata_all_file).read_text())
        dataset_name = dataset['name']

        log_str = "Trying to update {} with transformed dataset {}".format(metadata_all_file, dataset_name)
        self.logger.info(log_str)

        found_existing = False
        for i, data_structure in enumerate(metadata_all_json['dataStructures']):
            if data_structure['name'] == dataset['name']:
                metadata_all_json['dataStructures'][i] = dataset
                found_existing = True

        if not found_existing:
            metadata_all_json['dataStructures'].append(dataset)
            self.logger.info("Update successfull")
        else:
            self.logger.info("Did not update, {} already exists".format(dataset_name))

        metadata_all_file.write_text(json.dumps(metadata_all_json, sort_keys=False, indent=4, ensure_ascii=False))
