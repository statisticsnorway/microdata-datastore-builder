import logging
import json
from os import path
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
            metadata["unitType"] = DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(str(metadata["unitType"]["$ref"])))

        variable_idx = 0
        for variable in metadata["variables"]:
            if (variable["variableRole"] == "MEASURE"):
                if ("$ref" in variable["subjectFields"][0]):
                    subject_field_idx = 0
                    for subject_field in variable["subjectFields"]:
                        ref_to_subject_field = str(metadata["variables"][variable_idx]["subjectFields"][subject_field_idx]["$ref"])
                        metadata["variables"][variable_idx]["subjectFields"][subject_field_idx] = \
                            DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_subject_field))
                        subject_field_idx +=1
                if ("$ref" in variable["valueDomain"]):
                    ref_to_value_domain = str(metadata["variables"][variable_idx]["valueDomain"]["$ref"])
                    value_domain_new = DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_value_domain))
                    value_domain_current = metadata["variables"][variable_idx]["valueDomain"]
                    value_domain_new.update(value_domain_current)
                    value_domain_new.pop("$ref")  # remove old "$ref"
                    metadata["variables"][variable_idx]["valueDomain"] = value_domain_new

            elif (variable["variableRole"] == "IDENTIFIER") and ("$ref" in variable):
                ref_to_identifier = str(metadata["variables"][variable_idx]["$ref"])
                identifier_new = DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_identifier))
                metadata["variables"][variable_idx].update(identifier_new)
                metadata["variables"][variable_idx].pop("$ref")  # remove old "$ref"

            elif (variable["variableRole"] == "START_TIME") and ("$ref" in variable):
                ref_to_start = str(metadata["variables"][variable_idx]["$ref"])
                start_new = DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_start))
                metadata["variables"][variable_idx].update(start_new)
                metadata["variables"][variable_idx].pop("$ref")  # remove old "$ref"

            elif (variable["variableRole"] == "STOP_TIME") and ("$ref" in variable):
                ref_to_stop = str(metadata["variables"][variable_idx]["$ref"])
                stop_new = DatasetUtils.read_json_file(self.__data_input_root_path.joinpath(ref_to_stop))
                metadata["variables"][variable_idx].update(stop_new)
                metadata["variables"][variable_idx].pop("$ref")  # remove old "$ref"
            variable_idx += 1
        return metadata
        #print(json.dumps(metadata, indent=2, ensure_ascii=False))


#####################
### Usage example ###
#####################
di = DatasetInput(Path(r"C:\BNJ\prosjektutvikling\GitHub\statisticsnorway\microdata-datastore-builder\tests\resources\InputTestData\DataSet\KREFTREG_DS"))
metadata = di.build_metadata_dict()
print(json.dumps(metadata, indent=2, ensure_ascii=False))

