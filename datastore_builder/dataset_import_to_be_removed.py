#!/usr/bin/python3
import getopt
import logging.handlers
import subprocess
import random
import string
import sys

# from log_config import set_up_logging
# set_up_logging()
# log = logging.getLogger("dataset_import")


def versiontuple(v) -> tuple:
    return tuple(map(int, (v.split("."))))


def transformed_file(metadata_file):
    tmp = metadata_file.rsplit(".", 1)[0]
    return tmp + "_transformed.json"



# def create_run_id() -> str:
#     char_set = string.ascii_lowercase + string.digits
#     number = 6
#     return '{}-{}'.format(''.join(random.sample(char_set*number, number)), ''.join(random.sample(char_set*number, number)))

class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.

    Rather than use actual contextual information, we just use random
    data in this demo.
    """

    def create_run_id(self) -> str:
        char_set = string.ascii_lowercase + string.digits
        number = 6
        return '{}-{}'.format(''.join(random.sample(char_set * number, number)),
                              ''.join(random.sample(char_set * number, number)))

    def filter(self, record):
        record.runId = self.create_run_id()
        return True




def main(argv):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(runId)s - %(name)s - %(levelname)s  - %(message)s')
    log = logging.getLogger("dataset_import")

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

    command_list = []

    dataset_reader_str = "./reader_wrapper.py -d tests/resources/datasets/{} -m tests/resources/datasets/{} -v all -f \";\" -l 100"
    command_list.append(dataset_reader_str.format(data_file, metadata_file))

    metadata_transform_str = "./transformer_wrapper.py -i /Users/vak/temp/{} -o /Users/vak/temp/{}"
    command_list.append(metadata_transform_str.format(metadata_file, transformed_file(metadata_file)))

    update_metadata_all_str = "./updater_wrapper.py -i /Users/vak/temp/{} -o /Users/vak/temp/metadata-all__1_0_0.json"
    command_list.append(update_metadata_all_str.format(transformed_file(metadata_file)))

    count = 0
    for command in command_list:
        count += 1
        log_str = "Step {}: {}".format(count, command)
        insert_dash_line(len(log_str))
        log.info(log_str)
        insert_dash_line(len(log_str))
        command_with_parms = command.split(" ")
        sub = subprocess.run(command_with_parms)
        if sub.returncode != 0:
            log.error('Failed with ERROR!')
            exit(-1)


def insert_dash_line(length: int):
    log.info("-" * length)


if __name__ == "__main__":
    main(sys.argv[1:])
