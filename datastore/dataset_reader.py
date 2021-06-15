import logging
import json
#from os import path
from pathlib import Path
from typing import Union


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
    def __init__(self, dataset_path: Path):
        self.__data_input_root_path = dataset_path.parent.parent
        self.__dataset_path = dataset_path   # catalog with .csv datafile and .json metadatafile
        self.__dataset_data_file = self.__dataset_path.joinpath(str(self.__dataset_path.parts[-1]) + ".csv")
        self.__dataset_metadata_file = self.__dataset_path.joinpath(str(self.__dataset_path.parts[-1]) + ".json")


    def build_metadata_dict(self) -> dict:
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
        
        return metadata


#####################
### Usage example ###
#####################
# dataset_path = Path(r"C:\BNJ\prosjektutvikling\GitHub\statisticsnorway\microdata-datastore-builder\tests\resources\InputTestData\DataSet\KREFTREG_DS")
# di = DatasetInput(dataset_path)
# metadata = di.build_metadata_dict()
# DatasetUtils.write_json_file(metadata, dataset_path.joinpath("KREFTREG_DS_merged_result.json"))
# #print(json.dumps(metadata, indent=2, ensure_ascii=False))
