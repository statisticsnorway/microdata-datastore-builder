#!/usr/bin/python3
import getopt
import json
import logging.handlers
from pathlib import Path

import sys

from common import log_config, util
from transformer import Transformer
from updater import Updater


def version_tuple(v) -> tuple:
    return tuple(map(int, (v.split("."))))


def transformed_file(metadata_file):
    tmp = metadata_file.rsplit(".", 1)[0]
    return tmp + "_transformed.json"


WORKING_DIR = "/Users/vak/temp/"
METADATA_ALL_FILE = "metadata-all__1_0_0.json"


def main(argv):
    log_config.log_setup_for_import_pipeline()

    log = logging.getLogger("dataset_import")
    log_filter = log_config.ContextFilter(util.create_run_id())
    log.addFilter(log_filter)

    log.info('This is script dataset_import.py')
    log.info(sys.version_info)

    actual_version = '{}.{}.{}'.format(sys.version_info[0], sys.version_info[1], sys.version_info[2])
    if version_tuple(actual_version) < version_tuple("3.8.2"):
        log.error('Python version is {}, required minimum 3.8.2'.format(actual_version))
        raise Exception('Python version is {}, required minimum 3.8.2'.format(actual_version))

    metadata_file = ''
    data_file = ''
    try:
        opts, args = getopt.getopt(argv, "hm:d:", ["metadata=", "data="])
    except getopt.GetoptError:
        print('dataset_import.py -m <metadata_file> -d <data_file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Imports metadata and data to datastore\n' \
                  'dataset_import.py -m <metadata_file> -d <data_file>')
            sys.exit()
        elif opt in ("-m", "--metadata"):
            metadata_file = arg
        elif opt in ("-d", "--data"):
            data_file = arg

    log.info('Metadata: ' + metadata_file)
    log.info('Data: ' + data_file)

    metadata_path = WORKING_DIR + metadata_file
    transformed_metadata_path = transformed_file(metadata_path)

    run_transformer(metadata_path, transformed_metadata_path, log, log_filter)

    metadata_all_path = WORKING_DIR + METADATA_ALL_FILE
    run_updater(transformed_metadata_path, metadata_all_path, log, log_filter)


def metadata_path(metadata_file):
    return WORKING_DIR + metadata_file


def run_updater(input_file, output_file, log, log_filter):
    insert_dash_line(log)
    log.info("Calling Updater")
    log.info('input_file : ' + input_file)
    log.info('output_file : ' + output_file)
    updater = Updater(log_filter)
    updater.update_metadata_all(Path(input_file), Path(output_file))


def run_transformer(input_file, output_file, log, log_filter):
    insert_dash_line(log)
    log.info("Calling Transformer")
    log.info('input_file : ' + input_file)
    log.info('output_file : ' + output_file)
    dataset = json.loads(Path(input_file).read_text())
    transformer = Transformer(log_filter)
    transformed_dataset = transformer.transform_dataset(dataset)
    Path(output_file).write_text(json.dumps(transformed_dataset, sort_keys=False, indent=4, ensure_ascii=False))


def insert_dash_line(log):
    log.info("-" * 65)


if __name__ == "__main__":
    main(sys.argv[1:])
