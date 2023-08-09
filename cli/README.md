# CLI tool for vector

## Example tests

    - under the testing directory there are several example files to run tests against.

## Building CLI

From within the client directory run `poetry install`.

## Examples for Running Commands

### Creating a Table

` poetry run python -m cli create-table testing/create_table_test.toml`

### Ingesting Features

In this example the ingest_feature_list.json contains features to add to the product.
` poetry run python -m cli ingest <product_id> testing/ingest_feature_list.json`

### Listing Features

In this example the list_features_test.json file contains a json dict of filters to query on.
`poetry run python -m cli list-features <product_id> -f testing/list_features_test.json`

### Building the cli

Run the following commands:

### Install deps

`poetry install`

### Build the dist

`poetry build`

**NOTE**: this next command will overwrite the current installed version of descarteslabs_vector
`pip install --force-reinstall dist/descarteslabs_vector-0.1.0-py3-none-any.whl`

## Running from install

`python -m cli <command> <command_args>`
