# microdata-datastore-builder

A **lightweight datastore and backend data service** for microdata.no used for testing and development.

## Functionality included:
* Tools for building a new test datastore
* Functionality used to import and validate data (csv-files) and metadata (json-files)
* Functionality used to transform data and metadata to "Microdata format" (Swagger specification)
* A data/metadata backend service (Rest API). See project [microdata-test-datastore](https://github.com/statisticsnorway/microdata-test-datastore)

## Functionality **not included** in this lightweight datastore:
* No support for data versioning, patching and version bumping (use version 1.0.0 for all data/metadata)
* No support for data service security (Rest API) and JWT authentication
  * **WARNING! DO NOT USE THIS LIGHTWEIGHT DATASTORE FOR SENSITIVE DATA!**


# Getting started
## Install module from pip:
```  
pip install jsonschema
```

## Create a new datastore and add datasets (data and metadata)
See example in Jupyter notebook [example_create_new_datastore.ipynb](datastore/example_create_new_datastore.ipynb)

## Transform data and metadata to "Microdata format" (Swagger specification)
See the [transformer module](transformer/TRANSFORMER.md)

## The data/metadata Rest API
See project [microdata-test-datastore](https://github.com/statisticsnorway/microdata-test-datastore)

# Supported Microdata dataset input formats
## Dataset metadata format
JSON format specified by [JSON Schema for dataset metadata](datastore/JsonSchema_DataSet.json)

## Dataset data format
Use UTF8 character separated value files (csv).

*Column order in the datafile:*
```
"unit_id";"start-date";"stop-date";"value"
```
*Example data rows:*
```
111111111;1998-01-01;2020-12-31;AAA
222222222;2002-05-25;2021-02-10;BBB
333333333;1980-10-05;2015-07-18;CCC
```
