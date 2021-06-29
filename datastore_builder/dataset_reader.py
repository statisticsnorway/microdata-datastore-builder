import logging
import json
from pathlib import Path
from sqlite3.dbapi2 import Cursor
from typing import List, Tuple, Union
import datetime
import csv
import sqlite3 as db

import common.config as conf

# TODO: remove imports:
from time import gmtime, strftime


class DatasetUtils():
    # @staticmethod
    # def search_dictionary(var:Union[dict, list], search_key:str):
    #     """Takes a dict with nested lists and dicts,
    #     and searches all dicts (recursively) for the spesified search_key.
    #     """
    #     if isinstance(var, dict):
    #         for k, v in var.items():
    #             if k == search_key:
    #                 yield v
    #             if isinstance(v, (dict, list)):
    #                 yield from search_dictionary(v, search_key)
    #     elif isinstance(var, list):
    #         for d in var:
    #             yield from search_dictionary(d, search_key)

    @staticmethod
    def read_json_file(json_file: Path) -> Union[dict, None]:
        with open(json_file, encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception as e:
                logging.error("Not a valid JSON file: " + str(json_file) \
                    + "\n" + str(e)
                )
                return None

    @staticmethod
    def write_json_file(dict_obj: dict, json_file: Path):
        json_file.write_text(json.dumps(dict_obj, indent=4, ensure_ascii=False), encoding="utf-8")

    @staticmethod
    def create_temp_sqlite_db_file(db_file: Path) -> Tuple[db.Connection, db.Cursor]:
        if db_file.exists():
            db_file.unlink()  # removes a file or symbolic link

        sql_create_table = f"""\
            CREATE TABLE temp_dataset (
                unit_id TEXT NOT NULL, 
                value TEXT NOT NULL, 
                start TEXT, 
                stop TEXT, 
                attributes TEXT) """

        db_conn = db.connect(db_file) 
        cursor = db_conn.cursor()
        cursor.execute(sql_create_table)
        # Set Sqlite-params to speed up performance
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("BEGIN TRANSACTION")
        return (db_conn, cursor)


class DatasetInput():
    """Expected catalog structure for input data and metadata:
        <data_input_root catalog>
            /DataSet
                /DATASET_A
                    DATASET_A.json
                    DATASET_A.csv
                /DATASET_B
                    DATASET_B.json
                    DATASET_B.csv
                /DATASET_<XX>
            /Attribute
            /Identifier
            /SubjectField
            /UnitType
            /ValueDomain
    """

    def __init__(self, dataset_name: str, field_separator: str=";", data_error_limit: int=100) -> None:
        self.__dataset_name = dataset_name
        self.__field_separator = field_separator
        self.__data_error_limit = data_error_limit
        self.__data_input_root_path = Path(conf.DATA_INPUT_ROOT_DIR)
        self.__dataset_path = self.__data_input_root_path.joinpath("DataSet").joinpath(dataset_name)  # catalog with .csv datafile and .json metadatafile
        self.__dataset_data_file = self.__dataset_path.joinpath(dataset_name + ".csv")
        self.__dataset_metadata_file = self.__dataset_path.joinpath(dataset_name+ ".json")
        self.__metadata_dict = dict()
        self.__meta_temporal_coverage_start = None
        self.__meta_temporal_coverage_latest = None
        self.__meta_temporal_status_dates = set()


    def __build_metadata_dict(self):
        """
        Read the dataset JSON metadata file and include metadata-elements from external JSON-documents if "$ref" elements exists,
        eg. "$ref" to JSON metadata for ValueDomain, Identifier, SubjectFields, ...
        """
        metadata = DatasetUtils.read_json_file(self.__dataset_metadata_file)

        if "$ref" in metadata["unitType"]:
            ref_to_unit_type = str(metadata["unitType"]["$ref"])
            metadata["unitType"] = DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_unit_type))

        identifier_variable = [variable for variable in metadata["variables"] if variable.get("variableRole") == "IDENTIFIER"]
        if len(identifier_variable) > 0 and "$ref" in identifier_variable[0]:
            ref_to_identifier = identifier_variable[0]["$ref"]
            identifier_variable[0].update(DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_identifier)))
            identifier_variable[0].pop("$ref")  # remove old "$ref"

        measure_variable = [variable for variable in metadata["variables"] if variable.get("variableRole") == "MEASURE"]
        if len(measure_variable) > 0:
            for subject_field in measure_variable[0].get("subjectFields"):
                if "$ref" in subject_field:
                    ref_to_subject_field = subject_field["$ref"]
                    subject_field.update(DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_subject_field)))
                    subject_field.pop("$ref")  # remove old "$ref"
            
            value_domain = measure_variable[0].get("valueDomain")
            if "$ref" in value_domain:
                ref_to_value_domain = value_domain["$ref"]
                value_domain.update(DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_value_domain)))
                value_domain.pop("$ref")  # remove old "$ref"
                if "sentinelAndMissingValues" in value_domain:
                    # If "sentinelAndMissingValues" exists in JSON then move it to the end of valueDomain for better human readability
                    sentinel_and_missing_values = value_domain.pop("sentinelAndMissingValues")
                    value_domain.update({"sentinelAndMissingValues": sentinel_and_missing_values})

        start_time_variable = [variable for variable in metadata["variables"] if variable.get("variableRole") == "START_TIME"]
        if len(start_time_variable) > 0 and "$ref" in start_time_variable[0]:
            ref_to_start_time = start_time_variable[0]["$ref"]
            start_time_variable[0].update(DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_start_time)))
            start_time_variable[0].pop("$ref")  # remove old "$ref"

        stop_time_variable = [variable for variable in metadata["variables"] if variable.get("variableRole") == "STOP_TIME"]
        if len(stop_time_variable) > 0 and "$ref" in stop_time_variable[0]:
            ref_to_stop_time = stop_time_variable[0]["$ref"]
            stop_time_variable[0].update(DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_stop_time)))
            stop_time_variable[0].pop("$ref")  # remove old "$ref"

        self.__metadata_dict = metadata


    def __read_data_file(self):
        """Create a temp Sqlite db-file and read the dataset csv-datafile into a db-table"""
        sqlite_db_file = Path(conf.WORKING_DIR).joinpath(self.__dataset_name + ".db")
        temp_db = DatasetUtils.create_temp_sqlite_db_file(sqlite_db_file)
        db_conn = temp_db[0]
        cursor = temp_db[1]

        print("SQL Start:" + strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))
        with open(file=self.__dataset_data_file, newline='', encoding='utf-8') as f:
            #csv.register_dialect('my_dialect', delimiter=';', quoting=csv.QUOTE_NONE)
            reader = csv.reader(f, delimiter=self.__field_separator)
            cursor.executemany("INSERT INTO temp_dataset (unit_id, value, start, stop, attributes) VALUES (?, ?, ?, ?, ?)", reader)
        db_conn.commit()
        db_conn.close()
        print("SQL Ferdig: " + strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))


    def is_data_file_valid(self) -> bool:
        """Validate fields for each data row in the data file (unit_id, value, start, stop, attribute)"""

        print("Validate Start:" + strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))

        data_errors = []  # used for error-reporting
        rows_validated = 0
        with open(file=self.__dataset_data_file, newline='', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=self.__field_separator)
            try:
                for data_row in reader:
                    if reader.line_num % 1000000 == 0:
                        print("Rad: " + str(reader.line_num))

                    rows_validated += 1
                    if len(data_errors) >= self.__data_error_limit:
                        print(f"Error in file - {self.__dataset_data_file}")
                        print(f"  Error limit reached, {str(rows_validated)} rows validated")
                        for data_error in data_errors:
                            print(f"  {data_error}")
                        return False

                    # TODO: support for validation of attributes

                    if not data_row:
                        data_errors.append((reader.line_num, "Empty data row/line (Null/missing). Expected row with fields UNIT_ID, VALUE, (START), (STOP), (ATTRIBUTES))", None))
                    elif len(data_row) > 5:
                        data_errors.append((reader.line_num, "Too many elements in row/line. Expected row with fields UNIT_ID, VALUE, (START), (STOP), (ATTRIBUTES))", None))
                    else:
                        unit_id = data_row[0].strip('"')
                        value = data_row[1].strip('"')
                        start = data_row[2].strip('"')
                        stop = data_row[3].strip('"')
                        # TODO: attributes = data_row[4]

                        if unit_id == None or str(unit_id).strip(" ") == "":
                            data_errors.append((reader.line_num, "UNIT_ID (identifier) missing or null", unit_id))

                        if value == None or str(value).strip(" ") == "":
                            data_errors.append((reader.line_num, "VALUE (measure) missing or null", value))

                        if start not in(None, ""):
                            try:
                                datetime.datetime(int(start[:4]), int(start[5:7]), int(start[8:10]))
                            except:
                                data_errors.append((reader.line_num, "START-date not valid", start))

                        if stop not in(None, ""):
                            try:
                                datetime.datetime(int(stop[:4]), int(stop[5:7]), int(stop[8:10]))
                            except:
                                data_errors.append((reader.line_num, "STOP-date not valid", stop))

                        self.__metadata_set_temporal_dates(str(start).strip('"'), str(stop).strip('"'))
            except csv.Error as e:
                print('Error in file {}, line {}: {}'.format(self.__dataset_data_file, reader.line_num, e))
                return False

        print("Validate Ferdig: " + strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))

        if data_errors:
            print(f"Error in file - {self.__dataset_data_file}")
            print(f"  {str(rows_validated)} rows validated")
            for data_error in data_errors:
                print(f"  {data_error}")
            return False
        else:
            print(f"Datafile OK - {self.__dataset_data_file}")
            print(f"  {str(rows_validated)} rows validated")
            return True


    def __metadata_set_temporal_dates(self, start_date, stop_date):
        """Find oldest start-date and newest (latest) start/stop-date, and a list (a set()) with unique dates used in the dataset."""
        if start_date:
            if (not self.__meta_temporal_coverage_start) or (start_date < self.__meta_temporal_coverage_start):
                self.__meta_temporal_coverage_start = start_date
            if (not self.__meta_temporal_coverage_latest) or (start_date > self.__meta_temporal_coverage_latest):
                self.__meta_temporal_coverage_latest = start_date
            self.__meta_temporal_status_dates.add(start_date)  # set() with distinct start/stop-dates
        if stop_date:
            if (not self.__meta_temporal_coverage_latest) or (stop_date > self.__meta_temporal_coverage_latest):
                self.__meta_temporal_coverage_latest = stop_date
            self.__meta_temporal_status_dates.add(stop_date)  # set() with distinct start/stop-dates


    def __metadata_update_temporal_coverage(self) -> dict:
        """Update temporalCoverageStart, temporalCoverageLatest og temporalStatusDates"""
        if self.__metadata_dict["temporalityType"] in ("FIXED", "EVENT", "ACCUMULATED"): 
            self.__metadata_dict["dataRevision"]["temporalCoverageStart"] = self.__meta_temporal_coverage_start
            self.__metadata_dict["dataRevision"]["temporalCoverageLatest"] = self.__meta_temporal_coverage_latest
        elif self.__metadata_dict["temporalityType"] == "STATUS":
            temporalStatusDatesList = list(self.__meta_temporal_status_dates)
            temporalStatusDatesList.sort()
            self.__metadata_dict["dataRevision"]["temporalStatusDates"] = temporalStatusDatesList


    def run_reader(self):
        """Main run for DatasetInput class"""
        temp_dir = Path(conf.WORKING_DIR)

        #TODO: Hvis metadatafilen ikke inneholder ref kan den bare kopieres til temp-katalogen
        self.__build_metadata_dict()

        #TODO: Validate metadata-json with json-schema!!!!

        if self.is_data_file_valid():
            self.__read_data_file()

        if self.__meta_temporal_coverage_start or self.__meta_temporal_coverage_latest or self.__meta_temporal_status_dates:
            self.__metadata_update_temporal_coverage()

        DatasetUtils.write_json_file(self.__metadata_dict, temp_dir.joinpath(self.__dataset_name + ".json"))


#####################
### Usage example ###
#####################
dsi = DatasetInput("KREFTREG_DS")
dsi.run_reader()
#print(dsi.is_data_file_valid())



#####################
### Usage example ###
#####################
# dataset_path = Path(r"C:\BNJ\prosjektutvikling\GitHub\statisticsnorway\microdata-datastore-builder\tests\resources\InputTestData\DataSet\KREFTREG_DS")
# di = DatasetInput(dataset_path)
# metadata = di.build_metadata_dict()
# DatasetUtils.write_json_file(metadata, dataset_path.joinpath("KREFTREG_DS_merged_result.json"))
# #print(json.dumps(metadata, indent=2, ensure_ascii=False))
