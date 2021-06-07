import os
#import shutil
import pathlib
import json
import datetime

import datastores_config as ds_conf
import dataset_reader as dsr


class DatastoreBuilder:
    """
    Generate a new datastore.


    ##############################################
    ### Usage - how to create a new datastore. ###
    ##############################################

    1. Manually create a new empty directory on the server for your datastore (e.g. mkdir /my_path/test_datastore)

    2. Manually insert a new element in the file datastores_config.py (e.g. a new datastore with short-name "TEST")
       - Remember to set the correct "datastorePath" (the directory created in step 1).

    3. Example "create_new_datastore.py"
        # Creates a new empty datastore (directories, datastore.json, version.json and test-datasets)
        # - Set parameter include_testdata=True to create test datasets.

        dsb = DatastoreBuilder(
            datastore_short_name="TEST",
            include_testdata=True
        )
        
    """

    def __init__(self, datastore_short_name, include_testdata=True) -> None:
        self.datastore_short_name = datastore_short_name.upper()   # e.g. "TEST"
        self.include_testdata = include_testdata

        self.__data_store_name = ds_conf.datastore[self.datastore_short_name]["datastoreName"]   # e.g. "SSB test"
        self.__datastore_domain_name = ds_conf.datastore[self.datastore_short_name]["datastoreDomainName"]   # e.g. "no.ssb.test"
        self.__datastore_path = ds_conf.datastore[self.datastore_short_name]["datastorePath"]
        self.__datastore_default_language = ds_conf.datastore[self.datastore_short_name]["defaultLanguage"]
        
        self.__datastore_metadata_path = self.__datastore_path + "metadata/"
        self.__datastore_dataset_path = self.__datastore_path + "dataset/"

        self.__test_data_source_path = str(pathlib.Path(__file__).parent.parent) + "/tests/resources/"

        self.__create_datastore_directories()
        self.__generate_datastore_file()
        self.__generate_version_file()
        if include_testdata:
            self.__create_test_data()
        print(f'OK - datastore "{self.datastore_short_name}" created in {self.__datastore_path}')


    """Create datastore directories (sub folders)"""
    def __create_datastore_directories(self):
        os.mkdir(self.__datastore_metadata_path)
        os.mkdir(self.__datastore_dataset_path)


    """Generate "datastore.json" file."""
    def __generate_datastore_file(self):
        datastore_config = {
            "datastoreDomainName": self.__datastore_domain_name,
            "datastoreName": self.__data_store_name,
            "defaultLanguage": self.__datastore_default_language
        }  
        with open(self.__datastore_metadata_path + "datastore.json", "w", encoding="utf-8") as datastore_config_file:  
            json.dump(datastore_config, datastore_config_file, sort_keys=False, indent=4, separators=(',', ': '), ensure_ascii=False)


    """Generate datastore "version.json" file"""
    def __generate_version_file(self):
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
        with open(self.__datastore_metadata_path + "version.json", "w", encoding="utf-8") as datastore_version_file:  
            json.dump(datastore_version, datastore_version_file, sort_keys=False, indent=4, separators=(',', ': '), ensure_ascii=False)


    """Create test-Dataset (data and metadata) for PETS and INCOME"""
    def __create_test_data(self):
        ds_pets = dsr.DatasetReader(
            data_file = self.__test_data_source_path + "TEST_PERSON_PETS.txt",
            metadata_file = self.__test_data_source_path + "TEST_PERSON_PETS.json",
            validate = "all"
        )
        ds_pets.validate_dataset()
        ds_pets.create_new_dataset_in_datastore(datastore_short_name = self.datastore_short_name)

        ds_income = dsr.DatasetReader(
            data_file = self.__test_data_source_path + "TEST_PERSON_INCOME.txt",
            metadata_file = self.__test_data_source_path + "TEST_PERSON_INCOME.json",
            validate = "all"
        )
        ds_income.validate_dataset()
        ds_income.create_new_dataset_in_datastore(datastore_short_name = self.datastore_short_name)

### End class DataStoreBulder
