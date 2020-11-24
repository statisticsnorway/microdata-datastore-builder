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
        self.__database_temp_file = self.__file_directory + "/TEMP_" + str(self.__file_name).split(".")[0] + ".db"
        self.__db_connection = None
        self.__db_curs = None
        self.__data_errors = []


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

    """Read lines in the data file and insert as rows in a temporary Sqlite database (file)."""
    def read_csv_file(self):
        self.create_temp_database()
        sql_insert = """\
            INSERT INTO temp_data(unit_id, value, start, stop) VALUES (?, ?, ?, ?)
            """
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
                if rows_with_error == 0:
                    rows.append(row)
                    if i % 100000 == 0:
                        self.__db_curs.executemany(sql_insert, rows)   # Insert rows in batch to speed up writes to db.
                        rows = []
                        if i % 1000000 == 0:
                            print(".. rows validated: " + str(i))
            if rows_with_error == 0:
                self.__db_curs.executemany(sql_insert, rows)
                rows = []
            ### TODO: break if i == self.data_error_limit
        self.__db_connection.commit()
        self.__db_connection.close()
        if rows_with_error == 0:
            print("OK - " + str(i) +" rows/lines validated")
        else:
            print("ERROR: rows in datafile not valid:")
            for error in self.__data_errors:
                print("  " + "Line/row " + str(error[0]) + " - " + str(error[1]) + ": " + str(error[2]))
                self.delete_temp_database()  # Clean up

        #self.__db_curs.execute("SELECT * FROM temp_data ORDER BY unit_id, start")
        #self.__db_curs.execute("SELECT * FROM temp_data LIMIT 10")
        #for row in self.__db_curs:
        #    print(row)

        #self.__db_connection.commit()
        #self.__db_connection.close()
        #self.delete_temp_database()  # Clean up
        
        # validate_data_row
        # create_temp_database
        # insert_rows_to_db_table

    """Validate fields in each data row (unit_id, value, start, stop, attribute)."""
    def is_data_row_valid(self, data_row, row_number) -> bool:
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

        print(start)
        print(prev_stop)
        if (unit_id == prev_unit_id) and (start < prev_stop):
            self.__data_errors.append((row_number, "Previous STOP-date is greater than START-date", None))
            return False

        return True


    """Read and validate sorted data rows from temporary Sqlite database (file)."""
    def sort_and_validate_data_rows(self):
        if not self.__db_connection:
            self.__db_connection = db.connect(self.__database_temp_file)
            self.__db_curs = self.__db_connection.cursor()
        
        previous_row = None
        is_valid_event_history = True
        i = 1
        self.__db_curs.execute("SELECT * FROM temp_data ORDER BY unit_id, start")
        for row in self.__db_curs:
            i += 1
            if self.is_data_row_valid(row, i):
                if i >= 2:
                    is_valid_event_history = self.is_data_row_event_history_valid(row, previous_row, i)
            previous_row = row
        ## Alternative code using "cursor.fetchmany(number_of_rows)" reducing round trips to database.
        # while True:
        #     rows = self.__db_curs.fetchmany(100000)
        #     for row in rows:
        #         # some code ...
        #     if not rows:
        #         break
        #self.__db_connection.commit()
        self.__db_connection.close()



    # def insert_rows_to_db_table(self, row_list):
    #     None


    """Create temporary Sqlite database (file) and data table."""
    def create_temp_database(self):
        self.delete_temp_database()  # Remove old Sqlite3-file if exists
        self.__db_connection = db.connect(self.__database_temp_file)
        self.__db_curs = self.__db_connection.cursor()
        self.__db_curs.execute("CREATE TABLE IF NOT EXISTS temp_data (unit_id TEXT, value TEXT, start TEXT, stop TEXT)")
        #self.__db_curs.execute("CREATE TABLE IF NOT EXISTS temp_data (unit_id TEXT, value TEXT, start TEXT, stop TEXT, attributes TEXT)")
        # Speed up insert operations in Sqlite3
        self.__db_curs.execute("PRAGMA synchronous = OFF")
        self.__db_curs.execute("BEGIN TRANSACTION")


    """Clean up - delete temporary Sqlite database file when done."""
    def delete_temp_database(self):
        if os.path.exists(self.__database_temp_file):
            os.remove(self.__database_temp_file)


# TODO - tester her bare!!!!
    def sort_data(self):
        self.__db_connection = db.connect(self.__database_temp_file)
        self.__db_curs = self.__db_connection.cursor()
        self.__db_curs.execute("SELECT * FROM temp_data ORDER BY unit_id, start")
        i = 1
        for row in self.__db_curs:
            i += 1
            if i <= 10:
                print(row)

        # Alternative code using "cursor.fetchmany(number_of_rows)" reducing round trips to database.
        # while True:
        #     rows = self.__db_curs.fetchmany(100000)
        #     for row in rows:
        #         # some code ...
        #     if not rows:
        #         break
        
        print(str(i) + " rows sorted")
        self.__db_connection.close()


    # def temptemp(self):
    #     til = open(self.data_file + "_ny", "a")
    #     rows = []
    #     row = None
    #     i = 0
    #     with open(self.data_file, "r") as fp:
    #         for line in fp:
    #             i += 1
    #             row = line.split(self.field_separator)
    #             row = str(row[0]) + ";" + str(row[1]) + ";" + str(row[2]) + ";" + str(row[3])
    #             rows.append(row)
    #             if i % 100000 == 0:
    #                 til.writelines(rows)
    #                 rows = []
    #         til.writelines(rows)
    #     til.close()


# TODO: Skrive UNIT-tester !!!!!!

# Test
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/GitHub/statisticsnorway/microdata-datastore-builder/tests/resources/TEST_PERSON_INCOME__1_0.txt")
start = datetime.datetime.now()
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000_with_ERRORS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1000.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_1_million_STATUS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_10_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_10_million_STATUS.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_50_million.txt")
#sdr = SourceDataReader("C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp/testdata_50_million_STATUS.txt")

#sdr.read_csv_file()
#sdr.sort_data()
#print(start)
#print(datetime.datetime.now())