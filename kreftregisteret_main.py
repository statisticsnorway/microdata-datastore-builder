from pathlib import Path

import transformer as t

# 12 dataset/variables
# dataset_list = ["FODSELSAR", "KJONN", "DIAGNOSEAR", "DS", "LOKICD7", "TOPOGRAFIICDO3", "MORFOLOGIICDO3", "TOPOGRAFIICD10" "BASIS", "MET", "KIR", "ALDERDIAGNOSE"]
# 10 dataset/variables
dataset_list = ["FODSELSAR", "KJONN", "DIAGNOSEAR", "DS", "LOKICD7", "TOPOGRAFIICDO3", "BASIS", "MET", "KIR", "ALDERDIAGNOSE"]

version = "1_0_0"

input_path = 'tests/resources/TestDataStore/no_ssb_test/dataset/'
transformed_path = 'tests/resources/TestDataStore/no_ssb_test/dataset/'
metadata_all_path = 'tests/resources/TestDataStore/no_ssb_test/metadata/'

for dataset in dataset_list:
    input_dataset = Path(input_path + dataset + '/DOC__' + dataset + "__" + version +'.json')            # E.g. ./FOEDSELSAR/DOC__FOEDSELSAR__1_0_0.json
    transformed_dataset = Path(transformed_path + '/' + dataset + "/" + dataset + "__" + version + '_transformed.json')  # E.g. ./FOEDSELSAR/FOEDSELSAR__1_0_0_transformed.json

    t.transform_to_file(input_dataset, transformed_dataset)

    t.update_metadata_all_file(
        transformed_dataset,
        #Path('tests/resources/TestDataStore/no_ssb_test/metadata/metadata-all__1_0_0.json')
        Path(metadata_all_path + 'metadata-all__1_0_0.json')
    )
    print("OK - transformed: " + dataset)
