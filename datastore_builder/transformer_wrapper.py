#!/usr/bin/python3


# https://www.tutorialspoint.com/python/python_command_line_arguments.htm

import sys, getopt
import json
from pathlib import Path

from transformer import Transformer
import logging
from log_config import set_up_logging

set_up_logging()
log = logging.getLogger("transformer_wrapper")

# This should be moved to environment, PYTHONPATH
# new_path = '/Users/vak/projects/github/microdata-datastore-builder/transformer'
# if new_path not in sys.path:
#     sys.path.append(new_path)


def main(argv):
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
    transformer = Transformer()
    transformed_dataset = transformer.transform_dataset(dataset)
    Path(output_file).write_text(json.dumps(transformed_dataset, sort_keys=False, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1:])
