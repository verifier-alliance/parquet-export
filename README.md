# VerA DB Parquet Export Script

Python scripts and Docker container to export the Verifier Alliance PostgreSQL database in Parquet format and upload it to Google Cloud Storage.

The export script has undergone a redesign which made it append-only. The new export format is referred to as "v2". See https://github.com/argotorg/sourcify/issues/2441 for details of the redesign.

## Downloading the public dataset

The latest export is publicly available at [https://export.verifieralliance.org](https://export.verifieralliance.org).

Please refer to the [VerA docs](https://verifieralliance.org/docs/download) for instructions on how to download and use the Parquet files.

## Tests

Install the dependencies (see [Installation](#installation)) and `pytest`, then run the tests:

```
pip install pytest
python -m pytest tests
```

## Running the Export Script

### Requirements

- Python 3

### Installation

Create a virtual environment:

```
python -m venv venv
```

Activate the virtual environment:

```
source venv/bin/activate
```

Install dependencies:

```
pip install -r requirements.txt
```

### Usage

Run the script with:

```
python main.py
```

The script automatically detects existing files in GCS and performs **append-only exports**:

- **First run**: Exports all data from the database
- **Subsequent runs**:
  - Finds the newest file in GCS for each table (the file with the highest row range in its name)
  - Downloads it and reads the first row to determine the checkpoint
  - Regenerates the last file completely (in case it was incomplete)
  - Exports only new data that arrived since that checkpoint

### Removal of deleted rows

The export is append-only, so a row that is deleted in the database stays in the older files. With `REMOVAL_BUCKET_NAME` set, the script removes such rows before each export:

- It reads the removal lists at `{REMOVAL_PREFIX}/pending/*.json` in that private bucket (`REMOVAL_PREFIX` defaults to `parquet-removals`).
- It writes each older file that has a listed row again without the row, under the same name. So an older file can have fewer rows than the range in its name.
- It moves the handled lists to `{REMOVAL_PREFIX}/done/`.

A removal list has the `primary_key` values of the rows for each table, with binary values as `0x` hex:

```json
{
  "version": 1,
  "requestId": "3b241101-e2bb-4255-8caf-4136c566a962",
  "tables": {
    "verified_contracts": ["12345"],
    "sources": ["0x9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658"]
  }
}
```

### Debugging

The script takes some additional env vars for debugging purposes:

- `DEBUG`: Enables debug logging, reduces chunk sizes by 100x, processes only 1 file per table, and skips GCS upload
- `DEBUG_TABLE`: The name of the table to dump solely. Skips other tables.
- `DEBUG_OFFSET`: Will add an offset to the `SELECT` queries `"SELECT * FROM {table_name} OFFSET {os.getenv('DEBUG_OFFSET')}"`

The rest of the env vars can be found in the `.env-template` file. Copy the .env-template file to `.env` and fill in the values.

### Authentication

For local development, set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key JSON file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

In Cloud Run, authentication is automatic via Workload Identity.

### Configuration

The [config.py](./config.py) file contains the configuration for each database table including:

- `order_by`: The column to use for ordering data during export (typically `created_at`). Data is sorted by this column, with `primary_key` as a secondary sort for deterministic ordering.
- `primary_key`: The primary key column name (used as tie-breaker for append-only ordering)
- `datatypes`: Column type mappings for proper Parquet schema generation
- `chunk_size`: Number of rows to fetch per database query
- `num_chunks_per_file`: Number of chunks to write per Parquet file

Example:

```python
{
    'name': 'verified_contracts',
    'primary_key': 'id',
    'order_by': 'created_at',
    'datatypes': {
        'id': 'Int64',
        'created_at': 'datetime64[ns]',
        'updated_at': 'datetime64[ns]',
        'created_by': 'string',
        'updated_by': 'string',
        'deployment_id': 'string',
        'compilation_id': 'string',
        'creation_match': 'bool',
        'creation_values': 'json',
        'creation_transformations': 'json',
        'runtime_match': 'bool',
        'runtime_values': 'json',
        'runtime_transformations': 'json',
        'runtime_metadata_match': 'bool',
        'creation_metadata_match': 'bool'
    },
    'chunk_size': 100000,
    'num_chunks_per_file': 10
}
```

This config gives `100,000 * 10 = 1,000,000` rows per file.

The files will be named `verified_contracts_0_1000000.parquet`, `verified_contracts_1000000_2000000.parquet`, etc.

Files are stored in GCS under the `v2/{table_name}/` prefix and compressed using zstd.

### Docker

Build the image:

```
docker build --tag=kuzdogan/test-parquet-linux --platform=linux/amd64 .
```

Publish:

```
docker push kuzdogan/test-parquet-linux
```

## Metadata

Previously, the export script generated a `manifest.json` file containing metadata about the export. This is no longer generated, as we now rely on the Google Cloud Storage API for metadata.

For example, this API endpoint lists all available export v2 files with their metadata in XML:

https://export.test.verifieralliance.org/?prefix=v2/

This API is compatible with the AWS S3 API. Documentation can be found here: https://docs.cloud.google.com/storage/docs/xml-api/get-bucket-list
