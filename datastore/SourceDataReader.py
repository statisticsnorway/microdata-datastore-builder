import datetime
import json
import os
import random
import sqlite3 as db
from os import replace
from os.path import dirname

import datastores_config as ds_conf


#import shutil
#from os import path

from jsonschema import validate
from jsonschema.exceptions import ValidationError


class SourceDataReader:

    # TODO: Skrive flere UNIT-tester
    # TODO: Støtte attributes i sql-insert og innlesing av datafil?
    # TODO: Pseudonymisering
    # TODO: Støtte for DRAFT-datasett
    # TODO: Støtte for version bumping
    # TODO: support for startType and stopType?
    # TODO: Replace with PySpark SQL when migrating to SSB DAPLA.
    #       - Inkludert støtte for partisjonering av tid (start-dato)


    """TODO: Doc """
    # # Doc example:
    # def test(self, param1, param2) -> bool:
    #     """
    #     :param param1: First param bla, bla, bla, ..
    #     :type param1: string
    #     :param param2: Second param bla, bla, bla, ..
    #     :type param2: list
    #     :return: Doc bla, bla, bla, ..
    #     :rtype: bool
    #     """


    def __init__(self, data_file=None, metadata_file=None, validate="all", field_separator=";", data_error_limit=100) -> None:
        """
        Constructor.

        :param data_file: The character separated data file to read including path and file name, eg. ../my_catalog/my_dataset.sdv
        :param metadata_file: The metadata file (JSON) including path and name, eg. ../my_catalog/my_dataset.json
        :param validate: Legal values --> all|data|metadata. Validate data file, metadata file or all. Default is all.
        :param field_separator: Separator character for the data fields in the data file. Default is ; (semicolon).
        :param data_error_limit: Terminate the data validation program when reached the number (limit) of errors. Default is 100. 
        """
        self.data_file = data_file
        self.metadata_file = metadata_file
        self.validate = validate
        self.field_separator = field_separator
        self.data_error_limit = data_error_limit

        # TODO: hardcoded version for now, implement support for version bumping later
        self.__version_major = "1"
        self.__version_minor = "0"
        self.__version_patch = "0"
        
        self.__file_directory = None
        self.__file_name = None
        self.__sorted_temp_file = None
        self.__database_temp_file = None
        self.__database_table_name = None
        self.__db_connection = None
        self.__db_curs = None
        self.__data_errors = []

        self.__json_schema_dataset_file = os.path.dirname(os.path.realpath(__file__)) + "/JsonSchema_DataSet.json"
        self.__metadata_file = None
        self.__metadata_file_directory = None
        self.__metadata_file_name = None
        self.__metadata_file_temp = None
        self.__meta_value_domain_codes = None
        self.__meta_value_datatype = ""
        self.__meta_temporality_type = ""

        self.__meta_temporal_coverage_start = None
        self.__meta_temporal_coverage_latest = None
        self.__meta_temporal_status_dates = set()

        #self.set_and_validate_parameters()


    """Setter used by unit test!"""
    def set_meta_temporality_type(self, meta_temporality_type):
        self.__meta_temporality_type = meta_temporality_type


    """Check and set constructor parameters."""
    def set_and_validate_parameters(self) -> bool:
        if self.validate not in ("data", "metadata", "all"):
            print("ERROR in parameter 'validate'. Legal values: data, metadata or all")
            return False
        if self.validate in ("metadata", "all"):
            if not os.path.exists(str(self.metadata_file)):
                print("ERROR in parameter 'metadata_file'. Metadata file missig or wrong file path/name: " + str(self.metadata_file))
                return False
            else:
                self.__metadata_file = self.metadata_file
                self.__metadata_file_directory = os.path.dirname(self.metadata_file)
                self.__metadata_file_name = os.path.basename(self.metadata_file)
                self.__metadata_file_temp = self.metadata_file + "_temp"
                #self.__metadata_file = self.__file_directory + "/" + str(self.__file_name).split(".")[0] \
                #    + "_" + self.__version_patch + ".json"
        if self.validate in ("data", "all"):
            if not os.path.exists(str(self.data_file)):
                print("ERROR in parameter 'data_file'. Data file missig or wrong file path/name: " + str(self.data_file))
                return False
            else:
                self.__file_directory = os.path.dirname(self.data_file)
                self.__file_name = os.path.basename(self.data_file)
                self.__sorted_temp_file = self.__file_directory + "/" + str(self.__file_name).split(".")[0] + ".sorted"
                self.__database_table_name = str(self.__file_name).split(".")[0]
                self.__database_temp_file = self.__file_directory + "/" + self.__database_table_name + ".db"
                
        if self.validate == "all":
            # Check that metadata filename (.sdv/.txt) do match data filename (.json), eg. "my_dataset.sdv" and "my_dataset.json"
            if str(self.__file_name).split(".")[0] != str(self.__metadata_file_name).split(".")[0] :
                print("ERROR: Data filename do not match metadata filename:")
                print("    " + str(self.__file_name))
                print("    " + str(self.__metadata_file_name))
                return False
            # TODO: Check if filename isuppercase() ?

        self.delete_all_temp_files() # Cleanup - remove old temp files if exists.
        return True


    """Read and validate the lines/rows in the data file and insert as rows in a temporary Sqlite database (file)."""
    def read_csv_file(self) -> bool:
        print("Reading file " + self.data_file + " - " + str(datetime.datetime.now()))
        self.create_temp_database()
        # sql_insert = "INSERT INTO temp_data(part_num, unit_id, value, start, stop) VALUES (?, ?, ?, ?, ?)"
        sql_insert = f"INSERT INTO {self.__database_table_name} (part_num, unit_id, value, start, stop) VALUES (?, ?, ?, ?, ?)"
        # TODO: support for attributes  -->  sql_insert = f"INSERT INTO {self.__database_table_name}(part_num, unit_id, value, start, stop, attributes) VALUES (?, ?, ?, ?, ?, ?)"
        # TODO: support for startType and stopType?
        rows = []
        i = 0
        rows_with_error = 0
        with open(self.data_file, "r", encoding="utf-8") as fp:
            for line in fp:
                i += 1
                row = line.replace("\n", "").replace('"', '').split(self.field_separator)
                row.insert(0, random.randint(0, 999))  # part_num is a random integer between 0 and 999
                if not self.is_data_row_valid(row, i):
                    rows_with_error += 1
                    if rows_with_error >= self.data_error_limit:
                        self.__data_errors.append((0, "ERROR: Validation terminated. To many errors found!", None))
                        break  # exit loop if too many errors found
                if rows_with_error == 0:
                    row[3] = row[3].replace("-", "")  # Remove hyphen in start-date (cast to integer in Sqlite)
                    row[4] = row[4].replace("-", "")  # Remove hyphen in stop-date (cast to integer in Sqlite)
                    rows.append(row)
                    if i % 100000 == 0:
                        self.__db_curs.executemany(sql_insert, rows)   # Insert rows in batch to speed up writes to db.
                        rows = []
                        if i % 1000000 == 0:
                            print(".. rows read: " + str(i))
            if rows_with_error == 0:
                self.__db_curs.executemany(sql_insert, rows)
                rows = []
        self.__db_connection.commit()
        if rows_with_error == 0:
            print("  read " + str(i) +" rows/lines")
            return True  # OK - no error found in file
        else:
            self.print_data_errors()
            self.__db_connection.close()
            self.delete_temp_database()  # Clean up
            return False  # Errors


    """Validate fields in a data row (unit_id, value, start, stop, attribute)."""
    def is_data_row_valid(self, data_row, row_number) -> bool:
 
        # TODO: support for validation of attributes

        if not data_row:
            self.__data_errors.append((row_number, "Empty data line (Null/missing)", None))
            return False

        if len(data_row) < 4:
            self.__data_errors.append((row_number, "Row/line missing elements (expected line with fields UNIT_ID, VALUE, START, STOP, ..)", None))
            return False

        #part_num = data_row[0]
        unit_id = data_row[1]
        value = data_row[2]
        start = data_row[3]
        stop = data_row[4]
        # TODO: attributes = data_row[5]

        if unit_id == None or str(unit_id).strip(" ") == "":
            self.__data_errors.append((row_number, "UNIT_ID (identifier) missing or null", unit_id))
            return False

        if value == None or str(value).strip(" ") == "":
            self.__data_errors.append((row_number, "VALUE (measure) missing or null", value))
            return False

        if start not in(None, ""):
            try:
                datetime.datetime.strptime(str(start).strip('"'), "%Y-%m-%d")
            except:
                self.__data_errors.append((row_number, "START-date not valid", start))
                return False

        if stop not in(None, ""):
            try:
                datetime.datetime.strptime(str(stop).strip('"'), "%Y-%m-%d")
            except:
                self.__data_errors.append((row_number, "STOP-date not valid", stop))
                return False

        if (str(start).strip() in(None, "")) and str(stop).strip(" ")  not in(None, ""):
            self.__data_errors.append((row_number, "STOP-date exists, but START-date is missing", None))
            return False

        self.meta_set_temporal_objects(str(start).strip('"'), str(stop).strip('"'))

        return True


    """Read and validate sorted data rows from temporary Sqlite database (file). Write sorted data to file."""
    def sort_and_validate_data_rows(self) -> bool:

         # TODO: Replace with PySpark SQL when migrating to SSB DAPLA.

        is_meta_ok = self.meta_read_dataset_metadata()
        if not is_meta_ok:
            return False

        # if not self.__db_connection:
        #     self.__db_connection = db.connect(self.__database_temp_file)
        #     self.__db_curs = self.__db_connection.cursor()

        print("Sorting file " + self.data_file + " - " + str(datetime.datetime.now()))
        sorted_rows_to_write = []
        fpsort = open(self.__sorted_temp_file, 'w', encoding="utf-8")
        previous_row = None
        rows_with_error = 0
        i = 0
        ## Alternative code using "cursor.fetchmany(number_of_rows)" reducing round trips to database.
        # while True:
        #     rows = self.__db_curs.fetchmany(100000)
        #     for row in rows:
        #         # some code ...
        #     if not rows:
        #         break
        #self.__db_curs.execute("SELECT unit_id, value, start, stop FROM temp_data ORDER BY unit_id, start")
        self.__db_curs.execute( f"SELECT unit_id, value, start, stop FROM {self.__database_table_name} ORDER BY unit_id, start" )
        for row in self.__db_curs:
            i += 1
            if not self.is_data_row_consistent_with_metadata(row, i):
                rows_with_error += 1
                if rows_with_error >= self.data_error_limit:
                    self.__data_errors.append((0, "ERROR: Validation terminated. To many errors found!", None))
                    break  # exit loop if too many errors found
            if i >= 2 and not self.is_data_row_consistent(row, previous_row, i):
                rows_with_error += 1
                if rows_with_error >= self.data_error_limit:
                    self.__data_errors.append((0, "ERROR: Validation terminated. To many errors found!", None))
                    break  # exit loop if too many errors found
            previous_row = row
            sorted_rows_to_write.append(row)
            if i % 100000 == 0:
                for sorted_row in sorted_rows_to_write:
                    #fpsort.write(';'.join(sorted_row) + '\n')
                    #fpsort.write(";".join(str(sorted_row[0]), str(sorted_row[1]), str(sorted_row[2]), str(sorted_row[3])))
                    fpsort.write(";".join( str(element) for element in sorted_row ) + '\n')
                sorted_rows_to_write = []
                if i % 1000000 == 0:
                    print(".. rows written: " + str(i))
        for sorted_row in sorted_rows_to_write:
            #fpsort.write(';'.join(sorted_row) + '\n')
            fpsort.write(";".join( str(element) for element in sorted_row ) + '\n')
            sorted_rows_to_write = []
        fpsort.close()
        #self.__db_connection.commit()
        self.__db_connection.close()
        if rows_with_error == 0:
            print("  wrote " + str(i) + " sorted rows/lines")
            print("  Data file sorted. Consistency and event-history validation done - " + str(datetime.datetime.now()))
            return True # OK
        else:
            self.print_data_errors()
            return False


    """Validate consistency and event-history (unit_id * start * stop) and check for row duplicates."""
    def is_data_row_consistent(self, data_row, previous_data_row, row_number) -> bool:
        unit_id = data_row[0]
        value = data_row[1]
        start = data_row[2]
        stop = data_row[3]
        prev_unit_id = previous_data_row[0]
        prev_value = previous_data_row[1]
        prev_start = previous_data_row[2]
        prev_stop = previous_data_row[3]

        if data_row == previous_data_row:
            self.__data_errors.append((row_number, "Inconsistency - Data row duplicate", None))
            return False

        if (unit_id == prev_unit_id) and (start == prev_start):
            self.__data_errors.append((row_number, "Inconsistency - 2 or more rows with same UNIT_ID and START-date", None))
            return False

        # Valid temporalityTypes: "FIXED", "STATUS", "ACCUMULATED", "EVENT"
        if self.__meta_temporality_type in("STATUS", "ACCUMULATED", "EVENT"):
            if start == None or str(start).strip(" ") == "":
                self.__data_errors.append((row_number, "Inconsistency - START-date is missing. Expected START-date when DataSet.temporalityType is " + self.__meta_temporality_type, None))
                return False
            if (stop not in(None, "")) and (start > stop):
                self.__data_errors.append((row_number, "Inconsistency - START-date greater than STOP-date.", start + " --> " + stop))
                return False
            if self.__meta_temporality_type in("STATUS", "ACCUMULATED"):
                if str(stop).strip in(None, ""):
                    self.__data_errors.append((row_number, "Inconsistency - STOP-date is missing. Expected STOP-date when DataSet.temporalityType is " + self.__meta_temporality_type, None))
                    return False
            if self.__meta_temporality_type == "STATUS":
                if not start == stop:
                    self.__data_errors.append((row_number, "Inconsistency - expected same (equal) date for START-date and STOP-date when DataSet.temporalityType is " + self.__meta_temporality_type, None))
                    return False
            if self.__meta_temporality_type == "EVENT":
                if (unit_id == prev_unit_id) and (not prev_stop or prev_stop == ""):
                    self.__data_errors.append((row_number, "Inconsistency - previous row not ended (missing STOP-date in line/row " + str(row_number-1) + ")", None))
                    return False
                if (unit_id == prev_unit_id) and (start < prev_stop):
                    self.__data_errors.append((row_number, "Inconsistency - previous STOP-date is greater than START-date", None))
                    return False
        elif self.__meta_temporality_type == "FIXED":
            if unit_id == prev_unit_id:
                self.__data_errors.append((row_number, "Inconsistency - 2 or more rows with same UNIT_ID (data row duplicate) not legal when DataSet.temporalityType is " + self.__meta_temporality_type, None))
                return False
            elif start not in(None, ""):
                self.__data_errors.append((row_number, "Inconsistency - expected no START-date (should be MISSING/NULL) when DataSet.temporalityType is " + self.__meta_temporality_type, None))
                return False
            elif stop not in(None, ""):
                self.__data_errors.append((row_number, "Inconsistency - expected no STOP-date (should be MISSING/NULL) when DataSet.temporalityType is " + self.__meta_temporality_type, None))
                return False

        return True


    """Validate consistency between data and metadata"""
    def is_data_row_consistent_with_metadata(self, data_row, row_number) -> bool:
        #unit_id = data_row[0]
        value = data_row[1]
        #start = data_row[2]
        #stop = data_row[3]

        if self.__meta_value_domain_codes:
            # Enumerated value-domain for variable - check if value exsists in CodeList
            if str(value).strip('"') not in self.__meta_value_domain_codes:
                self.__data_errors.append((row_number, "Inconsistency - value (code) not in metadata ValueDomain/CodeList", value))
                return False
        #else:
            # Described value-domian for variable (e.g. amount, weight, length, ..)

        # "STRING", "LONG", "DOUBLE", "DATE"
        if self.__meta_value_datatype == "LONG":
            try:
                int(str(value).strip('"'))
                return True
            except:
                self.__data_errors.append((row_number, "Inconsistency - value not of type LONG", None))
                return False
        elif self.__meta_value_datatype == "DOUBLE":
            try:
                float(str(value).strip('"'))
                return True
            except:
                self.__data_errors.append((row_number, "Inconsistency - value not of type DOUBLE", None))
                return False
        elif self.__meta_value_datatype == "DATE":
            try:
                datetime.datetime.strptime(value, "%Y-%m-%d")
            except:
                self.__data_errors.append((row_number, "Inconsistency - value not of type DATE (YYYY-MM-DD)", None))
                return False

        return True


    """Update temporalCoverageStart, temporalCoverageLatest og temporalStatusDates"""
    def meta_update_temporal_coverage(self) -> dict:
        metadata = None
        with open(self.__metadata_file, mode="r", encoding="utf-8") as json_metadata_file: 
            metadata = json.load(json_metadata_file)
            metadata["dataRevision"]["temporalCoverageStart"] = self.__meta_temporal_coverage_start
            metadata["dataRevision"]["temporalCoverageLatest"] = self.__meta_temporal_coverage_latest
            if "temporalStatusDates" in metadata["dataRevision"]:
                temporalStatusDatesList = list(self.__meta_temporal_status_dates)
                temporalStatusDatesList.sort()
                metadata["dataRevision"]["temporalStatusDates"] = temporalStatusDatesList

        with open(self.__metadata_file_temp, mode="w", encoding="utf-8") as json_metadata_file_out: 
            json_meta_str = json.dumps(metadata, sort_keys=False, indent=4, separators=(',', ': '), ensure_ascii=False)
            json_metadata_file_out.write(json_meta_str)
            #print(json_meta, file=json_metadata_file_out)


    """Find oldest start-date and newest (latest) start/stop-date, and a list (a set()) with unique dates used in the dataset."""
    def meta_set_temporal_objects(self, start_date, stop_date):
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


    """Validate the metadata file (JSON) using JSON Schema."""
    def validate_metadata_file(self) -> bool:
        with open(self.__json_schema_dataset_file, mode="r", encoding="utf-8") as json_schema_dataset_file: 
            json_schema_dataset = json.load(json_schema_dataset_file)

        with open(self.__metadata_file, mode="r", encoding="utf-8") as dataset_metadata_json_file:
            dataset_metadata_json = json.load(dataset_metadata_json_file)

        try:
            # If no exception is raised by validate(), the instance is valid.
            validate(
                instance=dataset_metadata_json, 
                schema=json_schema_dataset
            )
            print("Metadata validated OK")
            return True
        except ValidationError as err:
            print()
            print("ERROR in metadata JSON-file: " + self.__metadata_file)
            print(err)
            return False


    """Read dataset metadata from JSON file."""
    def meta_read_dataset_metadata(self) -> bool:
        metadata = None
        if self.validate_metadata_file():
            with open(self.__metadata_file, mode="r", encoding="utf-8") as json_metadata_file: 
                metadata = json.load(json_metadata_file)

            # Read codeList if enumerated-valueDomain
            if "codeList" in metadata["measure"]["valueDomain"]:
                self.__meta_value_domain_codes = []
                for code_list in metadata["measure"]["valueDomain"]["codeList"]["topLevelCodeItems"]:
                    self.__meta_value_domain_codes.append(code_list["code"])

            self.__meta_value_datatype = metadata["measure"]["dataType"]
            self.__meta_temporality_type = metadata["temporalityType"]
            return True
        else:
            return False


    # TODO write sorted file or Sqlite-table to Parquet dataset?
    # def write_parquet_file(self):
    #     # Read from sorted Sqlite???
    #     None


    # TODO - pseudonymisering
    # def identifier_pseudonymization(self):
    #     None


    """Print data errors (if any)"""
    def print_data_errors(self):
        if len(self.__data_errors) > 0:
            print("Data errors found:")
            for error in self.__data_errors:
                print("  " + "Line/row " + str(error[0]) + " - " + str(error[1]), end='')
                if error[2]:
                    print(": " + str(error[2]), end='')
                print("")
        else:
            print("No data errors found")


    """Create temporary Sqlite database (file) and data table."""
    # TODO: Replace with PySpark SQL when migrating to SSB DAPLA.
    def create_temp_database(self):
        self.delete_temp_database()  # Remove old Sqlite3-file if exists
        #print(self.__database_temp_file)
        self.__db_connection = db.connect(self.__database_temp_file)
        self.__db_curs = self.__db_connection.cursor()

        # self.__db_curs.execute( \
        #     """CREATE TABLE IF NOT EXISTS temp_data (
        #         part_num INTEGER NOT NULL, 
        #         unit_id INTEGER NOT NULL, 
        #         value TEXT, 
        #         start INTEGER NOT NULL, 
        #         stop INTEGER NOT NULL, 
        #         attributes TEXT) """
        # )
        sql_create = f"""CREATE TABLE IF NOT EXISTS {self.__database_table_name} (
                part_num INTEGER NOT NULL, 
                unit_id INTEGER NOT NULL, 
                value TEXT, 
                start INTEGER NOT NULL, 
                stop INTEGER NOT NULL, 
                attributes TEXT) """
        self.__db_curs.execute(sql_create)

        # Set db-parameters to speed up insert operations in Sqlite3
        self.__db_curs.execute("PRAGMA synchronous = OFF")
        self.__db_curs.execute("BEGIN TRANSACTION")


    """Rename Sqlite database file and database table name"""
    def rename_database(self, new_name):
        self.__db_connection = db.connect(self.__database_temp_file)
        self.__db_curs = self.__db_connection.cursor()
        # 1. Rename database table
        self.__db_curs.execute( f"ALTER TABLE {self.__database_table_name} RENAME TO {new_name}" )
        # 2. Rename Sqlite database file
        try:
            self.__db_connection.close()
        except:
            None
        finally:
            os.rename(\
                self.__database_temp_file,
                self.__file_directory + "/" + new_name + ".db" )


    """Cleanup - delete temporary Sqlite.db file if exists."""
    def delete_temp_database(self):
        if os.path.exists(self.__database_temp_file):
            os.remove(self.__database_temp_file)


    """Cleanup - remove temporary DATA files if exists."""
    def delete_temp_data_files(self):
        if os.path.exists(self.__sorted_temp_file):
            os.remove(self.__sorted_temp_file)   # E.g, ../MY_DATA.sorted


    """Cleanup - remove temporary METADATA files if exists."""
    def delete_temp_metadata_files(self):
        if os.path.exists(self.__metadata_file_temp):
            os.remove(self.__metadata_file_temp)   # E.g, ../MY_DATA.json_temp


    """Cleanup - remove all temporary files."""
    def delete_all_temp_files(self):
        self.delete_temp_database()
        self.delete_temp_data_files()
        self.delete_temp_metadata_files()


    """Create a new dataset with datafile (Sqlite db) and metadatafile (json) in the data store."""
    def create_new_dataset_in_datastore(self, datastore_name : str):
        # TODO: support versions and version bumping
        datastore_path = ds_conf.datastore[datastore_name.upper()]["datastorePath"]

        dataset_name = str(self.__file_name).upper().split(".")[0]
        data_file_with_version = dataset_name + "__" + self.__version_major + "_" + self.__version_minor

        # TODO: Set access rights (chmod wr-r----) ?
        os.mkdir(datastore_path + "dataset" + "/" + dataset_name)

        self.rename_database(data_file_with_version) # rename Sqlite data file and database table
        data_file_from = self.__file_directory + "/" + data_file_with_version + ".db"
        data_file_to = datastore_path + "dataset" + "/" + dataset_name + "/" + data_file_with_version + ".db"
        os.rename(data_file_from, data_file_to)  # move Sqlite data file to DataStore

        metadata_file_with_version = "DOC__" + dataset_name + "__" + self.__version_major + "_" + self.__version_minor + "_" + self.__version_patch + ".json"
        metadata_file_from = self.__metadata_file_temp
        metadata_file_to = datastore_path + "dataset" + "/" + dataset_name + "/" + metadata_file_with_version
        os.rename(metadata_file_from, metadata_file_to)  # move metadata json file to DataStore



    """MAIN - Start validation of dataset"""
    def validate_dataset(self):
        if self.set_and_validate_parameters():
            if self.validate in("data", "all"):
                if self.read_csv_file():
                    print("Datafile OK")
                else:
                    print("ERROR: Validation terminated when reading data file. Too many errors found!")
                    return

            if self.validate == "metadata":
                if self.validate_metadata_file():
                    print("Metadata file OK")
                return

            if self.validate == "all":
                if self.sort_and_validate_data_rows():
                    self.meta_update_temporal_coverage()
                    self.delete_temp_data_files()
                    print("OK - data and metadata validated.")
                else:
                    print("ERROR: Consitency/event-history/metadata validation found errors in the data file and/or metadata JSON-file!")
                    print("  NB! If errors in data see the temporary sorted datafile for correct data line/row-number:")
                    print("  ---> " + str(self.__sorted_temp_file))

### END class SourceDataReader ###



### Usage examples ###
# Validate all (data file and metadata file and consistency)
# sdr = SourceDataReader(
#     data_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME.txt",
#     metadata_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME.json",
#     validate="all"
# )
# sdr.validate_dataset()

# ## Validate metadata file
# sdr = SourceDataReader(
#     data_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME.txt",
#     metadata_file=None,
#     validate="data"
# )
#sdr.validate_dataset()

# ## Validate data file
# sdr = SourceDataReader(
#     data_file=None,
#     metadata_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME.json",
#     validate="metadata"
# )
#sdr.validate_dataset()

# Validate the datafile and the metadatafile
sdr = SourceDataReader(
    data_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_PETS.txt",
    metadata_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_PETS.json",
    validate="all"
)
sdr.validate_dataset()

# Create the new dataset in the TEST-datastore (move the data and metadata to a new catalog in the TEST-datastore).
sdr.create_new_dataset_in_datastore(datastore_name="TEST") 


# sdr = SourceDataReader(
#     data_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME.txt",
#     metadata_file="C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME.json",
#     validate="all"
# )
# sdr.validate_dataset()
# sdr.create_new_dataset_in_datastore("TEST")


### Test run cases ###
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000_with_ERRORS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000_with_ERRORS_EVENT.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1_million_STATUS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_10_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_10_million_STATUS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_50_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_50_million_STATUS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/recources/TEST_PERSON_PETS__1_0.txt")

#sdr.data_error_limit = 100
#sdr.validate = "data"
#sdr.validate = "metadata"
#sdr.validate = "all"

#sdr.validate_dataset()







