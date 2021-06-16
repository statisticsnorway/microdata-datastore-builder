#!/usr/bin/python


# https://www.tutorialspoint.com/python/python_command_line_arguments.htm

import sys, getopt
import logging
from log_config import set_up_logging

set_up_logging()
log = logging.getLogger("dataset_reader")

# Fake dataset_reader, for demonstration purposes ONLY!

def main(argv):
    data_file = ''
    metadata_file = ''
    validate = ''
    field_separator = ''
    data_error_limit = ''
    try:
        opts, args = getopt.getopt(argv, "hd:m:v:f:l:",
                                   ["data_file=", "metadata_file=", "validate=", "field_separator=",
                                    "data_error_limit="])
    except getopt.GetoptError:
        print('dataset_reader.py -d <data_file> -m <metadata_file> -v <validate> -f <field_separator> -l '
              '<data_error_limit>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('dataset_reader.py -d <data_file> -m <metadata_file> -v <validate> -f <field_separator> -l '
                  '<data_error_limit>')
            sys.exit()
        elif opt in ("-d", "--data_file"):
            data_file = arg
        elif opt in ("-m", "--metadata_file"):
            metadata_file = arg
        elif opt in ("-v", "--validate"):
            validate = arg
        elif opt in ("-f", "--field_separator"):
            field_separator = arg
        elif opt in ("-l", "--data_error_limit"):
            data_error_limit = arg

    log.info('This is dataset_reader.py')
    log.info('data_file : ' + data_file)
    log.info('metadata_file : ' + metadata_file)
    log.info('validate : ' + validate)
    log.info('field_separator : ' + field_separator)
    log.info('data_error_limit : ' + data_error_limit)


if __name__ == "__main__":
    main(sys.argv[1:])
