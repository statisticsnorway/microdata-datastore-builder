#!/usr/bin/python3


import getopt
from pathlib import Path

import sys

from common import log_config, util
from updater import Updater


def main(argv):
    log = log_config.get_logger_for_import_pipeline("updater_wrapper")
    log_filter = log_config.ContextFilter(util.create_run_id())
    log.addFilter(log_filter)

    log.info('This is script updater_wrapper.py')
    log.info(sys.version_info)

    dataset_transformed_file = ''
    metadata_all_file = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        print('updater_wrapper.py -i <dataset_transformed_file> -o <metadata_all_file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Reads a transformed json file and updates metadata_all\n' \
                  'updater_wrapper.py -i <dataset_transformed_file> -o <metadata_all_file>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            dataset_transformed_file = arg
        elif opt in ("-o", "--ofile"):
            metadata_all_file = arg

    log.info('input_file : ' + dataset_transformed_file)
    log.info('output_file : ' + metadata_all_file)

    updater = Updater(log_filter)
    updater.update_metadata_all(Path(dataset_transformed_file), Path(metadata_all_file))


if __name__ == "__main__":
    main(sys.argv[1:])
