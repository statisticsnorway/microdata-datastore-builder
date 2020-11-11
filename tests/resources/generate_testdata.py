import string
import datetime
import random
import os

class MicrodataTestData:
    number_of_rows_in_testfile = 1000
    file_path = os.path.dirname(__file__)
    #file_path = os.path.dirname(os.path.realpath(__file__))
    include_header = True
    data_field_separator = ";"

    legal_code_values = ["MAN", "WOMAN", "TROLL"]
    min_numeric_value = 0
    max_numeric_value = 100000
    min_start_date = datetime.date(2000, 1, 1)
    max_stop_date = datetime.date(2020, 12, 31)
    attribute_fields = '{"quality="GOOD", "source"="FILE"}'  # Valid JSON or None

    __all_letters = string.ascii_letters
    __all_digits = string.digits

    def generate_test_dataset(self, file_name, value_type="CODE_VALUES"):
        print("Started: " + str(datetime.datetime.now()))
        # Example file:
        #   unit_id;value;start;stop;attributes
        #   "010150nnnnn";"0101";"1950-01-01";"2003-05-17";
        f = open(self.file_path + "/" + file_name, "w")  # Overwrie existing file content
        f.close()
        f = open(self.file_path + "/" + file_name, 'a')
        if self.include_header:
            file_header_line = "unit_id;value;start;stop"
            if self.attribute_fields:
                file_header_line += ";attribute"
            file_header_line = file_header_line.replace(";", self.data_field_separator)
            f.write(file_header_line + "\n")
        rows = []
        for i in range(0, self.number_of_rows_in_testfile):
            rows.append(self.generate_data_row(value_type))
            if(i>0 and i%500000 == 0):
                f.write("\n".join(rows) + "\n")
                rows = []
                print("Row: " + str(i))
        f.write("\n".join(rows))
        f.close()
        print("  " + str(self.number_of_rows_in_testfile) + " rows generated in file " + self.file_path + "/" + file_name)
        print("Done: " + str(datetime.datetime.now()))


    def generate_data_row(self, value_type="CODE_VALUES"):
        id = ''.join(random.choice(self.__all_digits) for i in range(14))
        row_start_date = self.__generate_date(self.min_start_date, self.max_stop_date)
        row_stop_date = self.__generate_date(row_start_date, self.max_stop_date)
        value = None
        if value_type == "CODE_VALUES":
            # Use code list as value
            value = self.legal_code_values[random.randint(0, len(self.legal_code_values)-1)]
        else:
            # "NUMERIC_VALUE"
            value = str(random.randrange(self.min_numeric_value, self.max_numeric_value))
        attribut = ""
        if self.attribute_fields:
            attribut = self.data_field_separator + self.attribute_fields
        row = \
            str(id) + self.data_field_separator \
            + value + self.data_field_separator \
            + row_start_date.strftime("%Y-%m-%d") + self.data_field_separator \
            + row_stop_date.strftime("%Y-%m-%d") \
            + attribut
        return row


    def __generate_date(self, start_date, end_date):
        if start_date == end_date:
            return start_date
        else:
            """Generate a random date between start_date and stop_date"""
            return self.__random_date_between_dates(start_date, end_date)


    def __random_date_between_dates(self, start_date, end_date):
        #start_date = datetime.date(2020, 1, 1)
        #end_date = datetime.date(2020, 12, 31)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + datetime.timedelta(days=random_number_of_days)
        return random_date


#####################
### Example usage ###
#####################

## Generate dataset with code-list values and temporalityType="STATUS" (snapshot data with same date for start and stop)
# mtd = MicrodataTestData()
# mtd.number_of_rows_in_testfile = 1000
# mtd.include_header = True
# mtd.data_field_separator = ";"
# mtd.attribute_fields = ""
# mtd.min_start_date = datetime.date(2020, 12, 31)  # Same start and stop for status-datasett
# mtd.max_stop_date = datetime.date(2020, 12, 31)
# mtd.legal_code_values = ["BANANA", "APPLE", "ORANGE"]
# mtd.generate_test_dataset(file_name="my_testdata.txt", value_type="CODE_VALUES")


## Generate dataset with numeric values and temporalityType="STATUS"
# mtd = MicrodataTestData()
# mtd.number_of_rows_in_testfile = 1000
# mtd.include_header = True
# mtd.data_field_separator = ";"
# mtd.attribute_fields = ""
# mtd.min_start_date = datetime.date(2020, 12, 31)  # Same start and stop for status-datasett
# mtd.max_stop_date = datetime.date(2020, 12, 31)
# mtd.min_numeric_value = 0
# mtd.max_numeric_value = 99999
# mtd.generate_test_dataset(file_name="my_testdata.txt", value_type="NUMERIC_VALUES")


## Generate dataset with code-list values and temporalityType="EVENT" (event history data wit start and stop/end date)
mtd = MicrodataTestData()
mtd.file_path = "C:/BNJ/prosjektutvikling/Python/micordata-datastore/temp"
mtd.number_of_rows_in_testfile = 1000
mtd.include_header = True
mtd.data_field_separator = ";"
mtd.attribute_fields = None
mtd.min_start_date = datetime.date(1970, 1, 1)  # Same start and stop for status-datasett
mtd.max_stop_date = datetime.date(2020, 12, 31)
mtd.legal_code_values = ["A", "B", "C", "D"]
mtd.generate_test_dataset(file_name="testdata_10_million.txt", value_type="CODE_VALUES")



