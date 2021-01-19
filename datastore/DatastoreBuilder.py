import os
#import shutil
import pathlib
import json
import datetime

import datastores_config as ds_conf
import DatasetReader as dsr


class DatastoreBuilder:
    """
    Generate a new datastore. Set parameter include_testdata=True to create test datasets.
    """

    def __init__(self, datastore_short_name, include_testdata=True) -> None:
        self.datastore_short_name = datastore_short_name.upper()   # e.g. "TEST"
        self.include_testdata = include_testdata

        self.__data_store_name = ds_conf.datastore[self.datastore_short_name]["datastoreName"]   # e.g. "SSB test"
        self.__datastore_domain_name = ds_conf.datastore[self.datastore_short_name]["datastoreDomainName"]   # e.g. "no.ssb.test"
        self.__datastore_path = ds_conf.datastore[self.datastore_short_name]["datastorePath"]
        self.__datastore_default_language = ds_conf.datastore[self.datastore_short_name]["defaultLanguage"]
        
        self.__datastore_metadata_path = self.__datastore_path + "metadata/"

        self.__test_data_source_path = str(pathlib.Path(__file__).parent.parent) + "/tests/resources/"

        self.generate_datastore_file()
        self.generate_version_file()
        if include_testdata:
            self.create_test_data()


    """Generate "datastore.json" file."""
    def generate_datastore_file(self):
        datastore_config = {
            "datastoreDomainName": self.__datastore_domain_name,  # E.g. no_ssb_test, no_nsd_test or no_kreftregisteret_test  
            "datastoreName": self.__data_store_name,
            "defaultLanguage": self.__datastore_default_language
        }  
        with open(self.__datastore_metadata_path + "datastore.json", "w", encoding="utf-8") as datastore_config_file:  
            json.dump(datastore_config, datastore_config_file, sort_keys=False, indent=4, separators=(',', ': '), ensure_ascii=False)


    """Generate datastore "version.json" file"""
    def generate_version_file(self):
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
    def create_test_data(self):
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


### Usage example:
# dsb = DatastoreBuilder(datastore_short_name="TEST", include_testdata=True)