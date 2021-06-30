import csv
import random
from pathlib import Path
from time import gmtime, strftime

number_of_rows_generated = 1000000
dataset_name = "KREFTREG_DS"
csv_file = Path(r"C:\BNJ\prosjektutvikling\GitHub\statisticsnorway\microdata-datastore-builder\tests\resources\InputTestData\DataSet").joinpath(dataset_name).joinpath(dataset_name + ".csv")

print(strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))

cnt = 0
with open(csv_file, mode='w', newline="") as data_file:
    data_writer = csv.writer(data_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for i in range(10000000000000, 10000000000000 + number_of_rows_generated):
        data_writer.writerow([i, str(random.randint(0,7)), "2020-01-01", "2020-12-31", ""])
        cnt += 1
        if cnt % 1000000 == 0:
            print("Rows generated: " + str(cnt))

print(strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))