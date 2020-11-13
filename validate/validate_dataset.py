import json
from jsonschema import validate
import os

current_directory = os.path.dirname(__file__)
json_schema_dataset_path = os.path.join(current_directory, 'JsonSchema_DataSet.json')
dataset_metadata_json_path = os.path.join(current_directory, 'TestMetadata_BEFOLKNING_BARN3_I_FAM.json')

json_schema_dataset_file = open(json_schema_dataset_path, encoding="utf-8") 
json_schema_dataset = json.load(json_schema_dataset_file)
#print(json_schema_dataset)

dataset_metadata_json_file = open(dataset_metadata_json_path, encoding="utf-8") 
dataset_metadata_json = json.load(dataset_metadata_json_file)
#print(dataset_metadata_json)
 
 # If no exception is raised by validate(), the instance is valid.
validate(
    instance=dataset_metadata_json, 
    schema=json_schema_dataset
)

print("Metadata validated OK")