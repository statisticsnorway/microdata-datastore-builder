import csv
from datetime import datetime

import pandas
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

from common import log_config


class Converter:

    def __init__(self, log_filter):
        self.logger = log_config.get_logger_for_import_pipeline("Converter")
        self.logger.addFilter(log_filter)
        self.logger.info('creating an instance of Converter')

    def days_since_epoch(self, date_string: str) -> int:
        epoch = datetime.utcfromtimestamp(0)
        date_obj = datetime.strptime(date_string, '%Y%m%d')
        return (date_obj - epoch).days

    def convert_from_csv_to_enhanced_csv(self, input_file: str, output_file: str) -> None:

        self.logger.info("Converts csv {} to enhanced csv {}".format(input_file, output_file))

        with open(input_file, newline='') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=';')
            with open(output_file, 'w', newline='') as target_file:
                csv_writer = csv.writer(target_file, delimiter=';', quoting=csv.QUOTE_MINIMAL)
                for row in csv_reader:
                    start_date: str = row[2]
                    stop_date: str = row[3]

                    start_date = start_date.replace('-', '')
                    stop_date = stop_date.replace('-', '')
                    row[2] = start_date
                    row[3] = stop_date

                    start_year = start_date[:4]
                    row.append(start_year)  # add start_year column to partition on it

                    if (start_date):
                        row.append(self.days_since_epoch(start_date))
                    else:
                        row.append('')

                    if (stop_date):
                        row.append(self.days_since_epoch(stop_date))
                    else:
                        row.append('')

                    csv_writer.writerow(row)

        self.logger.info("Convert to enhanced csv successfull")

    def convert_from_enhanced_csv_to_parquet(self, input_file: str, output_file: str) -> None:
        self.logger.info("Converts enhanced csv {} to parquet {}".format(input_file, output_file))

    def convert_from_enhanced_csv_to_partitioned_parquet(self, input_file: str, output_file: str,
                                                         parquet_partition_name: str) -> None:
        self.logger.info("Converts enhanced csv {} to partitioned parquet {} "
                         "with partition name {}".format(input_file, output_file, parquet_partition_name))

        csv_read_options = pv.ReadOptions(
            skip_rows=0,
            encoding="utf8",
            column_names=["unit_id", "value", "start", "stop", "start_year", "start_unix_days", "stop_unix_days"])

        csv_parse_options = pv.ParseOptions(delimiter=';')

        data_schema = pa.schema([
            pa.field(name='start_year', type=pa.string(), nullable=True),
            pa.field(name='unit_id', type=pa.uint64(), nullable=False),
            pa.field(name='value', type=pa.string(), nullable=False),
            pa.field(name='start_epoch_days', type=pa.int16(), nullable=True),
            pa.field(name='stop_epoch_days', type=pa.int16(), nullable=True),
        ])

        csv_convert_options = pv.ConvertOptions(column_types=data_schema,
                                                include_columns=["unit_id", "value", "start_year", "start_unix_days",
                                                                 "stop_unix_days"])

        table = pv.read_csv(input_file=input_file, read_options=csv_read_options, parse_options=csv_parse_options,
                            convert_options=csv_convert_options)

        #self.logger.info(table.nbytes)
        self.logger.info("Number of rows in csv file: {}".format(table.num_rows))
        # self.logger.info(table.schema)
        # self.logger.info(table.column_names)
        # pandas.set_option('max_columns', None)  # print all columns
        # self.logger.info(table.to_pandas().head(10))

        metadata_collector = []

        # write with partitions
        pq.write_to_dataset(table,
                            root_path=parquet_partition_name,
                            partition_cols=['start_year'],
                            metadata_collector=metadata_collector)

        self.logger.info("Convert to partitioned parquet successfull")
