#!/usr/bin/python3


import sys, getopt
import logging
from common import log_config, util

from reader import Reader


# Fake dataset_reader, for demonstration purposes ONLY!

def main(argv):

    log_config.log_setup_for_import_pipeline()

    log = logging.getLogger("reader_wrapper")
    log_filter = log_config.ContextFilter(util.create_run_id())
    log.addFilter(log_filter)

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
        print('reader_wrapper.py -d <data_file> -m <metadata_file> -v <validate> -f <field_separator> -l '
              '<data_error_limit>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('reader_wrapper.py -d <data_file> -m <metadata_file> -v <validate> -f <field_separator> -l '
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

    log.info('This is script reader_wrapper.py')
    log.info('data_file : ' + data_file)
    log.info('metadata_file : ' + metadata_file)
    log.info('validate : ' + validate)
    log.info('field_separator : ' + field_separator)
    log.info('data_error_limit : ' + data_error_limit)

    reader= Reader(log_filter)
    reader.hello()


if __name__ == "__main__":
    main(sys.argv[1:])
