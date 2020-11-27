from os.path import dirname
import sqlite3 as db
import os
import datetime

class SourceDataReader:
    """TODO: Doc """

    def __init__(self, data_file, dry_run=False, field_separator=";", data_error_limit=100) -> None:
        self.data_file = data_file
        self.dry_run = dry_run
        self.field_separator = field_separator
        self.data_error_limit = data_error_limit
        
        self.__file_directory = os.path.dirname(data_file)
        self.__file_name = os.path.basename(data_file)
        self.__sorted_temp_file = self.__file_directory + "/TEMP_" + str(self.__file_name).split(".")[0] + ".sorted"
        self.__database_temp_file = self.__file_directory + "/TEMP_" + str(self.__file_name).split(".")[0] + ".db"
        self.__db_connection = None
        self.__db_curs = None
        self.__data_errors = []


    # TODO: remove
    def test(self, param1, param2) -> bool:
        """TODO: Doc bla, bla, bla, ...

        :param param1: First param bla, bla, bla, ..
        :type param1: string
        :param param2: Second param bla, bla, bla, ..
        :type param2: list
        :return: Doc bla, bla, bla, ..
        :rtype: bool
        """
        None


    """Read and validate the lines/rows in the data file and insert as rows in a temporary Sqlite database (file)."""
    def read_csv_file(self) -> bool:
        print("Reading file " + self.data_file + " - " + str(datetime.datetime.now()))
        self.create_temp_database()
        sql_insert = """\
            INSERT INTO temp_data(unit_id, value, start, stop) VALUES (?, ?, ?, ?)
            """
        # TODO: support for attributes
        #sql_insert = """\
        #    INSERT INTO temp_data(unit_id, value, start, stop, attributes) VALUES (?, ?, ?, ?, ?)
        #    """
        rows = []
        i = 0
        rows_with_error = 0
        with open(self.data_file, "r") as fp:
            for line in fp:
                i += 1
                row = line.replace("\n", "").split(self.field_separator)
                if not self.is_data_row_valid(row, i):
                    rows_with_error += 1
                    if rows_with_error >= self.data_error_limit:
                        self.__data_errors.append((0, "ERROR: Validation terminated. To many errors found!", None))
                        break  # exit loop if too many errors found
                if rows_with_error == 0:
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

        unit_id = data_row[0]
        value = data_row[1]
        start = data_row[2]
        stop = data_row[3]

        if unit_id == None or str(unit_id).strip(" ") == "":
            self.__data_errors.append((row_number, "UNIT_ID (identifier) missing or null", unit_id))
            return False

        if value == None or str(value).strip(" ") == "":
            self.__data_errors.append((row_number, "VALUE (measure) missing or null", value))
            return False

        if start == None or str(start).strip(" ") == "":
            self.__data_errors.append((row_number, "START-date missing or null", start))
            return False
        else:
            try:
                datetime.datetime.strptime(start, "%Y-%m-%d")
            except:
                self.__data_errors.append((row_number, "START-date not valid", start))
                return False

        if stop not in(None, ""):
            try:
                datetime.datetime.strptime(stop, "%Y-%m-%d")
                if start > stop:
                    self.__data_errors.append((row_number, "START greater than STOP", stop))
            except:
                self.__data_errors.append((row_number, "STOP-date not valid", stop))
                return False

        return True


    """Validate event-history (unit_id * start * stop) and check for row duplicates."""
    def is_data_row_event_history_valid(self, data_row, previous_data_row, row_number) -> bool:
        unit_id = data_row[0]
        value = data_row[1]
        start = data_row[2]
        stop = data_row[3]
        prev_unit_id = previous_data_row[0]
        prev_value = previous_data_row[1]
        prev_start = previous_data_row[2]
        prev_stop = previous_data_row[3]

        if data_row == previous_data_row:
            self.__data_errors.append((row_number, "Data row duplicate", None))
            return False

        if (unit_id == prev_unit_id) and (start == prev_start):
            self.__data_errors.append((row_number, "2 or more rows with same UNIT_ID and START-date", None))
            return False

        if (unit_id == prev_unit_id) and (not prev_stop or prev_stop == ""):
            self.__data_errors.append((row_number, "Previous row not ended (missing STOP-date in line/row " + str(row_number-1) + ")", None))
            return False

        if (unit_id == prev_unit_id) and (start < prev_stop):
            self.__data_errors.append((row_number, "Previous STOP-date is greater than START-date", None))
            return False

        return True


    """Read and validate sorted data rows from temporary Sqlite database (file)."""
    def sort_and_validate_data_rows(self) -> bool:
        # if not self.__db_connection:
        #     self.__db_connection = db.connect(self.__database_temp_file)
        #     self.__db_curs = self.__db_connection.cursor()
        print("Sorting file " + self.data_file + " - " + str(datetime.datetime.now()))
        sorted_rows_to_write = []
        fpsort = open(self.__sorted_temp_file, 'w')
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
        self.__db_curs.execute("SELECT * FROM temp_data ORDER BY unit_id, start")
        for row in self.__db_curs:
            i += 1
            if i >= 2 and not self.is_data_row_event_history_valid(row, previous_row, i):
                rows_with_error += 1
                if rows_with_error >= self.data_error_limit:
                    self.__data_errors.append((0, "ERROR: Validation terminated. To many errors found!", None))
                    break  # exit loop if too many errors found
            previous_row = row
            sorted_rows_to_write.append(row)
            if i % 100000 == 0:
                for sorted_row in sorted_rows_to_write:
                    fpsort.write(';'.join(sorted_row) + '\n')
                sorted_rows_to_write = []
                if i % 1000000 == 0:
                    print(".. rows written: " + str(i))
        for sorted_row in sorted_rows_to_write:
            fpsort.write(';'.join(sorted_row) + '\n')
            sorted_rows_to_write = []
        fpsort.close()
        #self.__db_connection.commit()
        self.__db_connection.close()
        if rows_with_error == 0:
            print("  wrote " + str(i) + " sorted rows/lines")
            print("Data file sorted. Event-History validation done - " + str(datetime.datetime.now()))
            return True # OK
        else:
            self.print_data_errors()
            return False


    # TODO validering av konsistens mellom data og metadata
    def consistencyValidation():
        None


    # TODO oppdatere temporalCoverageStart og temporaleCoverageLatest
    # Bør flyttes til egen klasse
    def meta_update_temporale_coverage(self) -> dict:
        None

    # TODO lese json-metadata
    # Bør flyttes til egen klasse
    def meta_read_dataset_metadata(self):
        None

    # TODO write sorted file or Sqlite-table to Parquet dataset
    def write_parquet_file(self):
        # Read from sorted Sqlite???
        # or read from sorted csv-file???
        None


    # TODO - pseudonymisering
    def identifier_pseudonymization(self):
        None


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
    def create_temp_database(self):
        self.delete_temp_database()  # Remove old Sqlite3-file if exists
        self.__db_connection = db.connect(self.__database_temp_file)
        self.__db_curs = self.__db_connection.cursor()
        self.__db_curs.execute("CREATE TABLE IF NOT EXISTS temp_data (unit_id TEXT, value TEXT, start TEXT, stop TEXT)")
        # TODO 
        #self.__db_curs.execute("CREATE TABLE IF NOT EXISTS temp_data (unit_id TEXT, value TEXT, start TEXT, stop TEXT, attributes TEXT)")
        # Speed up insert operations in Sqlite3
        self.__db_curs.execute("PRAGMA synchronous = OFF")
        self.__db_curs.execute("BEGIN TRANSACTION")


    """Clean up - delete temporary Sqlite database file when done."""
    def delete_temp_database(self):
        if os.path.exists(self.__database_temp_file):
            os.remove(self.__database_temp_file)


    """Start validation of dataset (main program)"""
    def validate_dataset(self):
        if self.read_csv_file():
            if self.sort_and_validate_data_rows():
                print("OK")
            else:
                print("ERROR: Event-history validation found data inconsistency i data file!")
        else:
            #self.__data_errors.append((0, "ERROR: Validation terminated when reading file. To many errors found!", None))
            print("ERROR: Validation terminated when reading data file. To many errors found!")


# TODO: Skrive UNIT-tester !!!!!!

### Test cases ###
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME__1_0.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000_with_ERRORS.txt")
sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000_with_ERRORS_EVENT.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1_million_STATUS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_10_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_10_million_STATUS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_50_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_50_million_STATUS.txt")

sdr.data_error_limit = 100
sdr.validate_dataset()

