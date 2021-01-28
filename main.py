from pathlib import Path

import transformer as t

# t.transform_to_file()

t.update_metadata_all_file(
    {'name': 'TEST', 'title': 'Some title'},
    Path('tests/resources/TestDataStore/no_ssb_test/metadata/metadata-all__1_0_0.json')
)
