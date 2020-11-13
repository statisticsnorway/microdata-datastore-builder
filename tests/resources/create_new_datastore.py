import os
import json
import datetime
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import datastore_config as conf


datastore_name = "test"  # <--- Set your datastore name <---
include_testdata = True  # <--- Include testdataset pets and income? <---


datastore_directoy_name = str(conf.datastore_main_domain).replace(".", "_") + "_" + datastore_name.lower()  # E.g. no_ssb_test, no_nsd_test or no_kreftregisteret_test
datastore_root = os.path.join(conf.datastore_path, datastore_directoy_name)
datastore_metadata = os.path.join(datastore_root, "metadata")
datastore_data = os.path.join(datastore_root, "data")

# Generate "datastore.json" file.
def generate_datastore_file():
    datastore_config ={  
        "datastoreDomainName": conf.datastore_main_domain + "." + datastore_name.lower(),  # E.g. no_ssb_test, no_nsd_test or no_kreftregisteret_test  
        "datastoreName": datastore_name,
        "defaultLanguage": "no"
    }  
    with open(datastore_metadata + "/datastore.json", "w") as datastore_config_file:  
        json.dump(datastore_config, datastore_config_file, indent=4) 

# Generate datastore "version.json" file
def generate_version_file():
    datastore_version ={
        "versions": [ 
            {  
                "version": "1.0.0",
                "releaseTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "description": [
                    {"languageCode": "no", "value": "Første versjon av databasen."},
                    {"languageCode": "en", "value": "Datastore initial version."}
                ],
                "shortDescription": [
                    {"languageCode": "no", "value": "Første versjon."},
                    {"languageCode": "en", "value": "Initial version."}
                ]
            },
            {  
                "version": "0.0.0.1",
                "releaseTime": None,
                "description": [
                    {"languageCode": "no", "value": "DRAFT"},
                    {"languageCode": "en", "value": "DRAFT"}
                ],
                "shortDescription": [
                    {"languageCode": "no", "value": "DRAFT"},
                    {"languageCode": "en", "value": "DRAFT"}
                ]
            }
        ]
    }  
    with open(datastore_metadata + "/version.json", "w") as datastore_version_file:  
        json.dump(datastore_version, datastore_version_file, indent=4) 

# Generate test data files "test_data_pets__1_0.parquet" and "test_data_income__1_0.parquet"
def generate_test_data():
    # test_dataset_parquet_schema = pa.schema([
    #     ('unit_id', pa.string()),
    #     ('value', pa.string()),
    #     ('start', pa.date32()),
    #     ('stop', pa.date32())
    #     #('attributes', pa.????)
    # ])
    fields = [
        ('unit_id', pa.string()),
        ('value', pa.string()),
        ('start', pa.date32()),
        ('stop', pa.date32())
        #('attributes', pa.????)
    ]
    data_schema = pa.schema(fields)

    #csv_parse_options = pv.ParseOptions(delimiter=';', header_rows=0)
    csv_parse_options = pv.ParseOptions(delimiter=';')
    csv_convert_options = pv.ConvertOptions(column_types=data_schema)
    if include_testdata:
        current_directory = os.path.dirname(__file__)
        # pets dataset
        datastore_data_pets = os.path.join(datastore_data, "test_data_pets")
        test_file_pets = "test_data_pets__1_0"
        os.mkdir(datastore_data_pets)
        table_pets = pv.read_csv(input_file=current_directory + "/" + test_file_pets+".txt", parse_options=csv_parse_options, convert_options=csv_convert_options)
        pq.write_table(table_pets, datastore_data_pets + "/" + test_file_pets+".parquet")
        # income dataset
        datastore_data_income = os.path.join(datastore_data, "test_data_income")
        os.mkdir(datastore_data_income)
        test_file_income = "test_data_income__1_0"
        table_income = pv.read_csv(input_file=current_directory + "/" + test_file_income+".txt", parse_options=csv_parse_options, convert_options=csv_convert_options)
        pq.write_table(table_income, datastore_data_income + "/" + test_file_income+".parquet")

        #writer = pq.ParquetWriter('parquest_user_defined_schema.parquet', schema=test_dataset_parquet_schema)
        #writer = pq.ParquetWriter('parquest_user_defined_schema.parquet', schema=test_dataset_parquet_schema)
        #writer.write_table(table_pets, current_directory + "/" + test_file_pets+".parqet")
        #writer.close()


# Generate test metadata files "test_metadata_pets__1_0_0.json" and "test_metadata_income__1_0_0.json"
def generate_test_metadata():
    # TODO
    if include_testdata:
        None

# Create a new datastore
def create_new_datastore():
    if not os.path.exists(datastore_root):
        # Create datastore parent directory (root) and sub-folders
        os.mkdir(datastore_root)
        # create sub-folders
        if not os.path.exists(datastore_metadata):
            os.mkdir(datastore_metadata)
        if not os.path.exists(datastore_data):
            os.mkdir(datastore_data)
        generate_datastore_file()
        generate_version_file()
        generate_test_data()
        generate_test_metadata()
        print("OK - DataStore '% s' created" % datastore_root)
    else:
        print("ERROR: DataStore '% s' not created! Datastore already exists." % datastore_root)


## Main - create new empty datastore catalog
if __name__ == "__main__":
    create_new_datastore()