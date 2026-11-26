# VerA DB Parquet Export Script

Python scripts and Docker container to export the Verifier Alliance PostgreSQL database in Parquet format and upload it to Google Cloud Storage.

The export script has undergone a redesign which made it append-only. The new export format is referred to as "v2". See https://github.com/argotorg/sourcify/issues/2441 for details of the redesign.

## Downloading the public dataset

The latest export is publicly available at [https://export.verifieralliance.org](https://export.verifieralliance.org).

Please refer to the [VerA docs](https://verifieralliance.org/docs/download) for instructions on how to download and use the Parquet files.

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

The export is append-only. So a row that is deleted in the database stays in the older files. To remove such rows, the script can read removal lists before each export.

Set these env vars to turn the removal step on:

- `REMOVAL_BUCKET_NAME`: a private bucket with the removal lists. Without it, the script skips the step.
- `REMOVAL_PREFIX`: the folder of the lists in that bucket. Default: `parquet-removals`.

A removal list is a JSON file at `{REMOVAL_PREFIX}/pending/{name}.json`:

```json
{
  "version": 1,
  "requestId": "3b241101-e2bb-4255-8caf-4136c566a962",
  "tables": {
    "verified_contracts": ["12345"],
    "contracts": ["0b1c6e2e-7c1a-4a5e-9d0c-2f6f1f0e8a11"],
    "sources": ["0x9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658"]
  }
}
```

A key is the value of the `primary_key` column of the table in [config.py](./config.py): integers and UUIDs as text, binary values as `0x` hex. The script ignores a table that is not in the export.

For each table of the lists, the script:

1. reads the key column of the older files and finds the files with a listed row;
2. writes each of these files again without the rows, with the same schema, row groups, and compression, and uploads it under the same name;
3. skips the newest file, because the export then writes that file again from the database.

After all tables are done, it moves each list to `{REMOVAL_PREFIX}/done/`. If the step fails, the lists stay in `pending/` and the next run handles them again.

Consequences for users of the dataset:

- An older file can change, and it can then have fewer rows than the range in its name.
- The newest file of a table is the file with the highest row range in its name. The script does not use the last modified time for this, because the removal step changes the time of an older file.

The service account of the script needs the right to read, create, and delete objects in the removal folder.

Run the tests of the removal step with:

```
pip install pytest
python -m pytest tests
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
