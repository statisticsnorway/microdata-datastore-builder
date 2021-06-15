#!/usr/bin/python3


# https://www.tutorialspoint.com/python/python_command_line_arguments.htm

import sys, getopt
from pathlib import Path

import logging
from log_config import set_up_logging

set_up_logging()
log = logging.getLogger("update_metadata_all")

# This should be moved to environment, PYTHONPATH
# new_path = '/Users/vak/projects/github/microdata-datastore-builder/transformer'
# if new_path not in sys.path:
#     sys.path.append(new_path)

import updater


def main(argv):
    dataset_transformed_file = ''
    metadata_all_file = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        print('update_metadata_all.py -i <dataset_transformed_file> -o <metadata_all_file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Reads a transformed json file and updates metadata_all\n' \
                  'update_metadata_all.py -i <dataset_transformed_file> -o <metadata_all_file>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            dataset_transformed_file = arg
        elif opt in ("-o", "--ofile"):
            metadata_all_file = arg

    log.info('Dette er update_metadata_all.py')
    log.info('input_file : ' + dataset_transformed_file)
    log.info('output_file : ' + metadata_all_file)

    updater.Updater.update_metadata_all(Path(dataset_transformed_file), Path(metadata_all_file))

if __name__ == "__main__":
    main(sys.argv[1:])
