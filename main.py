import transformer as t

read_from = 'tests/resources/TEST_PERSON_PETS.json'
write_to = 'tests/resources/TEST_PERSON_PETS_transformed.json'

t.transform_to_file(read_from, write_to)

#t.update_metadata_all_file(None, None)