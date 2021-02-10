# microdata-datastore-builder
Backend service for building a complete datastore with data and metadata.

## Transformer module
Transforms metadata of a dataset into NSD information model. See [example usage](main.py).
Exposes also an **update_metadata_all_file** method to add or update the dataset in a metadata 
file that contains all transformed datasets ("metadata_all").
This file will be returned on API call to /metadata/all endpoint in test-datastore.

### Example usage of transformer module
```python
import transformer as t
from pathlib import Path

input_dataset = Path('path/to/DATASET.json')
transformed_dataset = Path('path/to/DATASET_transformed.json')
metadata_all = Path('path/to/metadata-all__1_0_0.json')

#Convert a dataset
t.transform_to_file(input_dataset, transformed_dataset)

#Update metadata_all file
t.update_metadata_all_file(transformed_dataset,metadata_all)
```