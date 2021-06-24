#!/usr/bin/python3
import getopt
import json
from pathlib import Path

import sys

from common import log_config, util
from common.config import WORKING_DIR, METADATA_ALL_FILE
from reader import Reader
from transformer import Transformer
from updater import Updater
from converter import Converter


def version_tuple(v) -> tuple:
    return tuple(map(int, (v.split("."))))


def main(argv):
    log = log_config.get_logger_for_import_pipeline("dataset_import")
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

    run_reader(log, log_filter)

    metadata_path = WORKING_DIR + metadata_file
    transformed_metadata_path = metadata_path.replace('.json', '_transformed.json')

    run_transformer(metadata_path, transformed_metadata_path, log, log_filter)

    metadata_all_path = WORKING_DIR + METADATA_ALL_FILE
    run_updater(transformed_metadata_path, metadata_all_path, log, log_filter)

    converter = Converter(log_filter)
    data_path = WORKING_DIR + data_file
    enhanced_data_path = data_path.replace('.csv', '_enhanced.csv')
    run_csv_converter(converter, data_path, enhanced_data_path, log, log_filter)


    # csv_filename = "accumulated_data_300_million_rows_converted.csv"
    # parquet_filename = '../data/' + csv_filename.replace('csv', 'parquet')
    # parquet_partition_name = '../data/' + csv_filename.replace('.csv', '')

    temporality_type = get_temporality_type(metadata_path, log)
    parquet_filename = enhanced_data_path.replace('_enhanced.csv', '.parquet')
    run_parquet_converter(converter, enhanced_data_path, parquet_filename, temporality_type, log, log_filter)


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


def run_reader(log, log_filter):
    insert_dash_line(log)
    log.info("Calling Reader")
    reader = Reader(log_filter)
    reader.hello()


def run_csv_converter(converter, input_file, output_file, log, log_filter):
    insert_dash_line(log)
    log.info("Calling Converter")
    log.info('input_file : ' + input_file)
    log.info('output_file : ' + output_file)
    converter.convert_from_csv_to_enhanced_csv(input_file, output_file)


def run_parquet_converter(converter, input_file, output_file, temporality_type, log, log_filter):
    insert_dash_line(log)
    log.info("Calling Converter")
    log.info('input_file : ' + input_file)
    log.info('output_file : ' + output_file)
    if temporality_type in ["STATUS", "ACCUMULATED"]:
        parquet_partition_name = output_file.replace('.parquet', '')
        converter.convert_from_enhanced_csv_to_partitioned_parquet(input_file, output_file, parquet_partition_name)
    else:
        converter.convert_from_csv_to_enhanced_csv(input_file, output_file)


def get_temporality_type(input_file, log):
    dataset = json.loads(Path(input_file).read_text())
    temporality_type = dataset['temporalityType']
    log.info("Dataset {} temporalityType {}".format(dataset['shortName'], temporality_type))
    return temporality_type


def insert_dash_line(log):
    log.info("-" * 65)


if __name__ == "__main__":
    main(sys.argv[1:])
