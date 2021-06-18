#!/usr/bin/python3


import getopt
import json
from pathlib import Path

import sys

from common import log_config, util
from transformer import Transformer


def main(argv):
    log = log_config.get_logger_for_import_pipeline("transformer_wrapper")
    log_filter = log_config.ContextFilter(util.create_run_id())
    log.addFilter(log_filter)

    log.info('This is script transformer_wrapper.py')
    log.info(sys.version_info)

    input_file = ''
    output_file = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        print('transformer_wrapper.py -i <input_file> -o <output_file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Reads a json file, transforms it according to NSD swagger spesification ' \
                  'and stores the result into output file\n' \
                  'transformer_wrapper.py -i <input_file> -o <output_file>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file = arg
        elif opt in ("-o", "--ofile"):
            output_file = arg

    log.info("This is script transformer_wrapper.py")
    log.info('input_file : ' + input_file)
    log.info('output_file : ' + output_file)

    dataset = json.loads(Path(input_file).read_text())
    transformer = Transformer(log_filter)
    transformed_dataset = transformer.transform_dataset(dataset)
    Path(output_file).write_text(json.dumps(transformed_dataset, sort_keys=False, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1:])
