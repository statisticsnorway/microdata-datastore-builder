from pathlib import Path

import transformer as t

read_from = 'tests/resources/TEST_PERSON_PETS.json'
write_to = 'tests/resources/TEST_PERSON_PETS_transformed.json'

t.transform_to_file(read_from, write_to)

t.update_metadata_all_file(
    {'name': 'TEST', 'title': 'Some title'},
    Path('tests/resources/TestDataStore/no_ssb_test/metadata/metadata-all__1_0_0.json')
)
