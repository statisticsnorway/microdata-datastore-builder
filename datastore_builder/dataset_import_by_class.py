#!/usr/bin/python3
import getopt
import json
import logging.handlers
import random
import string
from pathlib import Path

import sys

from transformer import Transformer


def versiontuple(v) -> tuple:
    return tuple(map(int, (v.split("."))))


def transformed_file(metadata_file):
    tmp = metadata_file.rsplit(".", 1)[0]
    return tmp + "_transformed.json"


class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """
    run_id = ""

    def __init__(self, run_id):
        self.run_id = run_id

    def filter(self, record):
        record.runId = self.run_id
        return True


def create_run_id() -> str:
    char_set = string.ascii_lowercase + string.digits
    number = 6
    return '{}-{}'.format(''.join(random.sample(char_set * number, number)),
                          ''.join(random.sample(char_set * number, number)))


def main(argv):

    log_format = '%(asctime)s - %(runId)s - %(name)s - %(levelname)s  - %(message)s'

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))
    stream_handler.setLevel(logging.INFO)

    logging.basicConfig(filename='/Users/vak/temp/dataset_import.log', format=log_format, level=logging.DEBUG)
    logging.getLogger('').addHandler(stream_handler)

    log = logging.getLogger("dataset_import")
    run_id_filter = ContextFilter(create_run_id())
    log.addFilter(run_id_filter)

    log.info('This is script dataset_import.py')
    log.info(sys.version_info)

    actual_version = '{}.{}.{}'.format(sys.version_info[0], sys.version_info[1], sys.version_info[2])
    if versiontuple(actual_version) < versiontuple("3.8.2"):
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

    working_dir = "/Users/vak/temp/"

    command_list = []

    # dataset_reader_str = "./reader_wrapper.py -d tests/resources/datasets/{} -m tests/resources/datasets/{} -v all -f \";\" -l 100"
    # command_list.append(dataset_reader_str.format(data_file, metadata_file))

    # metadata_transform_str = "./transformer_wrapper.py -i /Users/vak/temp/{} -o /Users/vak/temp/{}"
    # command_list.append(metadata_transform_str.format(metadata_file, transformed_file(metadata_file)))

    input_file = working_dir + metadata_file
    output_file = transformed_file(working_dir + metadata_file)

    log.info("Calling Transformer")
    log.info('input_file : ' + input_file)
    log.info('output_file : ' + output_file)

    dataset = json.loads(Path(input_file).read_text())
    transformer = Transformer(run_id_filter)
    # transformer = Transformer()
    transformed_dataset = transformer.transform_dataset(dataset)
    Path(output_file).write_text(json.dumps(transformed_dataset, sort_keys=False, indent=4, ensure_ascii=False))

    # update_metadata_all_str = "./updater_wrapper.py -i /Users/vak/temp/{} -o /Users/vak/temp/metadata-all__1_0_0.json"
    # command_list.append(update_metadata_all_str.format(transformed_file(metadata_file)))

    # count = 0
    # for command in command_list:
    #     count += 1
    #     log_str = "Step {}: {}".format(count, command)
    #     insert_dash_line(len(log_str))
    #     log.info(log_str)
    #     insert_dash_line(len(log_str))
    #     command_with_parms = command.split(" ")
    #     sub = subprocess.run(command_with_parms)
    #     if sub.returncode != 0:
    #         log.error('Failed with ERROR!')
    #         exit(-1)


# def insert_dash_line(length: int):
#     log.info("-" * length)


if __name__ == "__main__":
    main(sys.argv[1:])
