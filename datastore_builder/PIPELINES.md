# Datastore builder
## Pipelines

---

### P1 dataset_import.py
This pipeline prepares and imports data and metadata into the datastore.
```shell
dataset_import.py -m <metadata_file> -d <data_file>
```
It runs the following steps in this order:
#### Reader (reader.py)
Reads, validates and enhances data and metadata.
#### Transformer (transformer.py)                                                   
Transforms metadata of a dataset into NSD information model.
#### Updater (updater.py)                                             
Adds the dataset in a metadata file that contains all transformed datasets (**"metadata_all"**).
  
***The steps above may be run individually using python scripts as wrappers:***
#### Reader
```shell
reader_wrapper.py -d <data_file> -m <metadata_file> -v <validate> -f <field_separator> -l <data_error_limit>
```
#### Transformer
```shell
transformer_wrapper.py -i <input_file> -o <output_file>
```
#### Updater
```shell
updater_wrapper.py -i <dataset_transformed_file> -o <metadata_all_file>
```

---
### P2 dataset_release_operation.py
To be continued ...

---
### P3 datastore_version_bumper.py
To be continued ...

---
## Logging
All pipelines log to the stdout and a logfile.
Example from dataset import:
```
2021-06-21 10:11:02,792 - 8eie33-ol2uag - dataset_import - INFO  - This is script dataset_import.py
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - sys.version_info(major=3, minor=8, micro=2, releaselevel='final', serial=0)
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - Metadata: KREFTREG_DS_described.json
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - Data: KREFTREG_DS.csv
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - -----------------------------------------------------------------
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - Calling Reader
2021-06-21 10:11:02,793 - 8eie33-ol2uag - Reader - INFO  - creating an instance of Reader
2021-06-21 10:11:02,793 - 8eie33-ol2uag - Reader - INFO  - Hello world
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - -----------------------------------------------------------------
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - Calling Transformer
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - input_file : /Users/vak/temp/KREFTREG_DS_described.json
2021-06-21 10:11:02,793 - 8eie33-ol2uag - dataset_import - INFO  - output_file : /Users/vak/temp/KREFTREG_DS_described_transformed.json
2021-06-21 10:11:02,794 - 8eie33-ol2uag - Transformer - INFO  - creating an instance of Transformer
2021-06-21 10:11:02,794 - 8eie33-ol2uag - Transformer - INFO  - Transforms KREFTREG_DS, STATUS, 2015-01-01, 2020-12-31
2021-06-21 10:11:02,797 - 8eie33-ol2uag - Transformer - INFO  - Finished transformation
2021-06-21 10:11:02,798 - 8eie33-ol2uag - dataset_import - INFO  - -----------------------------------------------------------------
2021-06-21 10:11:02,798 - 8eie33-ol2uag - dataset_import - INFO  - Calling Updater
2021-06-21 10:11:02,798 - 8eie33-ol2uag - dataset_import - INFO  - input_file : /Users/vak/temp/KREFTREG_DS_described_transformed.json
2021-06-21 10:11:02,798 - 8eie33-ol2uag - dataset_import - INFO  - output_file : /Users/vak/temp/metadata-all__1_0_0.json
2021-06-21 10:11:02,798 - 8eie33-ol2uag - Updater - INFO  - creating an instance of Updater
2021-06-21 10:11:02,799 - 8eie33-ol2uag - Updater - INFO  - Trying to update /Users/vak/temp/metadata-all__1_0_0.json with transformed dataset KREFTREG_DS
2021-06-21 10:11:02,799 - 8eie33-ol2uag - Updater - INFO  - Did not update, KREFTREG_DS already exists
```
More than one users may be running the pipeline at the same time.

We have introduced a unique run-id (**8eie33-ol2uag** in the example above) in order to keep track of the logs
coming from the same thread/process. 
## Configuration
The constants in **common/config.py** must be adapted to the deployment environment:
```shell
WORKING_DIR = "/Users/vak/temp/"
LOG_FILE_FOR_IMPORT_PIPELINE = "/Users/vak/temp/dataset_import.log"
METADATA_ALL_FILE = "metadata-all__1_0_0.json"
```
