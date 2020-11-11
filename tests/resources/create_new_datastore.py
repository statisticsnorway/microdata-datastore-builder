import os
import datastore_config as conf

datastore_name = "test"  # <-- set datastore name

# Create datastore parent directory (root) and sub-folders
datastore_directoy_name = str(conf.datastore_main_domain).replace(".", "_") + "_" + datastore_name.lower()  # E.g. no_ssb_test, no_nsd_test or no_kreftregisteret_test
datastore_root = os.path.join(conf.datastore_path, datastore_directoy_name)
if not os.path.exists(datastore_root):
    os.mkdir(datastore_root)
    # Create sub-folders
    datastore_metadata = os.path.join(datastore_root, "metadata")
    if not os.path.exists(datastore_metadata):
        os.mkdir(datastore_metadata)
    datastore_data = os.path.join(datastore_root, "data")
    if not os.path.exists(datastore_data):
        os.mkdir(datastore_data)
        print("DataStore '% s' created" % datastore_root)
else:
    print("ERROR: DataStore '% s' not created! Datastore already exists." % datastore_root)

