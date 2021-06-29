#import logging
import json
from pathlib import Path
from typing import List, Tuple, Union
import datetime
import sqlite3 as db

import common.config as conf
from common.dataset_utils import DatasetUtils


# TODO:
# Create temp catalog
# Validate lines sdv
# Read sdv, write Sqlite to /temp
# Sort Sqlite
# Validate consistency
# Update temporalCoverage-start-stop-list
# Write to stage catalog????


class DatasetValidator:

    # TODO: Skrive loggfil (til loggtjeneset)
    # TODO: Skrive flere UNIT-tester
    # TODO: Støtte attributes i sql-insert og innlesing av datafil?
    # TODO: Pseudonymisering
    # TODO: Støtte for DRAFT-datasett
    # TODO: Støtte for version bumping
    # TODO: support for startType and stopType?
    # TODO: Replace with PySpark SQL when migrating to SSB DAPLA.
    #       - Inkludert støtte for partisjonering av tid (start-dato)


    def __init__(self, dataset_name: str, data_error_limit: int=100) -> None:
        """
        Constructor.

        :param dataset_name: The name of the dataset, eg. "PERSON_INCOME"
        :param data_error_limit: Terminate the data validation program when reached the number (limit) of errors. Default is 100.
        """
        self.__dataset_name = dataset_name
        self.__data_error_limit = data_error_limit
        self._data_errors = []  # used for error-reporting
        self.__sqlite_db_file = Path(conf.WORKING_DIR).joinpath(self.__dataset_name + ".db")
        self.__metadata = DatasetUtils.read_json_file(Path(conf.WORKING_DIR).joinpath(self.__dataset_name + ".json"))
        self.__metadata_temporality_type = self.__metadata["temporalityType"]
        self.__metadata_measure_datatype = self.__get_metadata_measure_variable()["dataType"]
        self.__metadata_measure_code_list = self.__get_metadata_measure_code_list()
        self.__metadata_measure_sentinel_missing_values = self.__get_metadata_measure_sentinel_missing_values()


    def __get_metadata_measure_variable(self):
        return [variable for variable in self.__metadata["variables"] if variable.get("variableRole") == "MEASURE"][0]


    def __get_metadata_measure_code_list(self) -> Union[list, None]:
        """get codeList if exists in enumerated-valueDomain"""
        meta_value_domain_codes = None
        #if "codeList" in self.__metadata["measure"]["valueDomain"]:
        if "codeList" in self.__get_metadata_measure_variable()["valueDomain"]:
            meta_value_domain_codes = []
            #for code_list in self.__metadata["measure"]["valueDomain"]["codeList"]["topLevelCodeItems"]:
            for code_list_item in self.__get_metadata_measure_variable()["valueDomain"]["codeList"]["codeItems"]:
                meta_value_domain_codes.append(code_list_item["code"])
        return meta_value_domain_codes


    def __get_metadata_measure_sentinel_missing_values(self) -> Union[list, None]:
        meta_missing_values = None
        if "sentinelAndMissingValues" in self.__get_metadata_measure_variable()["valueDomain"]:
            meta_missing_values = []
            for missing_item in self.__get_metadata_measure_variable()["valueDomain"]["sentinelAndMissingValues"]:
                meta_missing_values.append(missing_item["code"])
        return meta_missing_values


    def validate_data(self) -> bool:
        """Read and validate sorted data rows from the temporary Sqlite database file (sorted by unit_id, start, stop)"""
        temp_db = DatasetUtils.read_temp_sqlite_db_data_sorted(self.__sqlite_db_file)
        db_conn = temp_db[0]
        cursor = temp_db[1]

        row_number = 0
        previous_data_row = (None, None, None, None, None)
        data_errors = 0
        for data_row in cursor:   # data-rows in cursor sorted by unit_id, start, stop
            row_number += 1
            if not self.__is_data_row_consistent(data_row, previous_data_row, row_number):
                data_errors += 1
            if not self.__is_data_row_consistent_with_metadata(data_row, row_number):
                data_errors += 1
            previous_data_row = data_row
        db_conn.close()
        return data_errors


    def __is_data_row_consistent(self, data_row, previous_data_row, row_number) -> bool:
        """Validate consistency and event-history (unit_id * start * stop) and check for row duplicates."""
        unit_id = data_row[0]
        value = data_row[1]
        start = data_row[2]
        stop = data_row[3]
        attributes = data_row[4]
        prev_unit_id = previous_data_row[0]
        prev_value = previous_data_row[1]
        prev_start = previous_data_row[2]
        prev_stop = previous_data_row[3]
        prev_attributes = previous_data_row[4]

        if data_row == previous_data_row:
            self.__data_errors.append((row_number, "Inconsistency - Data row duplicate", None))
            return False

        if (unit_id == prev_unit_id) and (start == prev_start):
            self.__data_errors.append((row_number, "Inconsistency - 2 or more rows with same UNIT_ID and START-date", None))
            return False

        # Valid temporalityTypes: "FIXED", "STATUS", "ACCUMULATED", "EVENT"
        if self.__metadata_temporality_type in("STATUS", "ACCUMULATED", "EVENT"):
            if start == None or str(start).strip(" ") == "":
                self.__data_errors.append((row_number, "Inconsistency - START-date is missing. Expected START-date when DataSet.temporalityType is " + self.__metadata_temporality_type, None))
                return False
            if (stop not in(None, "")) and (start > stop):
                self.__data_errors.append((row_number, "Inconsistency - START-date greater than STOP-date.", start + " --> " + stop))
                return False
            if self.__temporality_type in("STATUS", "ACCUMULATED"):
                if str(stop).strip in(None, ""):
                    self.__data_errors.append((row_number, "Inconsistency - STOP-date is missing. Expected STOP-date when DataSet.temporalityType is " + self.__metadata_temporality_type, None))
                    return False
            if self.__temporality_type == "STATUS":
                if not start == stop:
                    self.__data_errors.append((row_number, "Inconsistency - expected same (equal) date for START-date and STOP-date when DataSet.temporalityType is " + self.__metadata_temporality_type, None))
                    return False
            if self.__temporality_type == "EVENT":
                if (unit_id == prev_unit_id) and (not prev_stop or prev_stop == ""):
                    self.__data_errors.append((row_number, "Inconsistency - previous row not ended (missing STOP-date in line/row " + str(row_number-1) + ")", None))
                    return False
                if (unit_id == prev_unit_id) and (start < prev_stop):
                    self.__data_errors.append((row_number, "Inconsistency - previous STOP-date is greater than START-date", None))
                    return False
        elif self.__metadata_temporality_type == "FIXED":
            if unit_id == prev_unit_id:
                self.__data_errors.append((row_number, "Inconsistency - 2 or more rows with same UNIT_ID (data row duplicate) not legal when DataSet.temporalityType is " + self.__metadata_temporality_type, None))
                return False
            #elif start not in(None, ""):
            #    self.__data_errors.append((row_number, "Inconsistency - expected no START-date (should be MISSING/NULL) when DataSet.temporalityType is " + self.__metadata_temporality_type, None))
            #    return False
            #elif stop not in(None, ""):
            #    self.__data_errors.append((row_number, "Inconsistency - expected no STOP-date (should be MISSING/NULL) when DataSet.temporalityType is " + self.__metadata_temporality_type, None))
            #    return False
        return True


    def __is_data_row_consistent_with_metadata(self, data_row, row_number) -> bool:
        """Validate consistency between data and metadata"""
        #unit_id = data_row[0]
        value = data_row[1]
        #start = data_row[2]
        #stop = data_row[3]
        #attributes = data_row[4]

        if self.__metadata_measure_code_list:
            # Enumerated value-domain for variable - check if value exsists in CodeList
            if str(value).strip('"') not in self.__metadata_measure_code_list + self.__metadata_measure_sentinel_missing_values:
                self.__data_errors.append((row_number, "Inconsistency - value (code) not in metadata ValueDomain/CodeList or SentinelAndMissingValues-list", value))
                return False
        #else:
            # Described value-domian for variable (e.g. amount, weight, length, ..)

        # Valid datatypes: "STRING", "LONG", "DOUBLE", "DATE"
        if self.__metadata_measure_datatype == "LONG":
            try:
                int(str(value).strip('"'))
                return True
            except:
                self.__data_errors.append((row_number, "Inconsistency - value not of type LONG", None))
                return False
        elif self.__metadata_measure_datatype == "DOUBLE":
            try:
                float(str(value).strip('"'))
                return True
            except:
                self.__data_errors.append((row_number, "Inconsistency - value not of type DOUBLE", None))
                return False
        elif self.__metadata_measure_datatype == "DATE":
            try:
                #datetime.datetime.strptime(value, "%Y-%m-%d")
                datetime.datetime(int(value[:4]), int(value[5:7]), int(value[8:10]))
            except:
                self.__data_errors.append((row_number, "Inconsistency - value not of type DATE (YYYY-MM-DD)", None))
                return False
        return True



#####################
### Usage example ###
#####################
dsv = DatasetValidator("KREFTREG_DS")
print(dsv.validate_data())
#dsv.run_validator()
