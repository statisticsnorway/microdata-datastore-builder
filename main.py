from pathlib import Path

import transformer as t

input_dataset = Path('tests/resources/TEST_PERSON_PETS.json')
transformed_dataset = Path('tests/resources/TEST_PERSON_PETS_transformed.json')

t.transform_to_file(input_dataset, transformed_dataset)

t.update_metadata_all_file(
    transformed_dataset,
    Path('tests/resources/TestDataStore/no_ssb_test/metadata/metadata-all__1_0_0.json')
)
