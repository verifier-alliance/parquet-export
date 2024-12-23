# VerA DB Parquet Export Script

Python scripts and Docker container to export the Verifier Alliance PostgreSQL database in Parquet format and upload it to an S3 bucket.

Warning: The script likely has some memory leak issues and needs some refinement. Still, does the job.

~~The latest export is available on [Cloudflare R2](https://pub-f4b5a1306ebd42a3b1289ab59da1d9bf.r2.dev/manifest.json).~~ The database with the new v1 schema is still in the making. We'll announce when these new exports are publicly available. Bear in mind the data is not production-ready and contains many mistakes. We've laid out some of the problems [in this document](https://efdn.notion.site/VerA-DB-Problems-and-Changes-9a6873b2c0cc4b9c9cb04c82f6a0745b?pvs=4)

## Requirements

- Python 3

## Installation

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

## Usage

Run the script with:

```
python main.py
```

The script takes some additional env vars for debugging purposes:

- `DEBUG_TABLE`: The name of the table to dump solely. Skips other tables.
- `DEBUG_OFFSET`: Will add an offset to the `SELECT` queries `"SELECT * FROM {table_name} OFFSET {os.getenv('DEBUG_OFFSET')}"`

The rest of the env vars can be found in the `.env-template` file. Copy the .env-template file to `.env` and fill in the values.

The [config.py](./config.py) file contains the configuration for each database table about the chunk sizes and number of chunks per file, and the datatypes for each column in the table.

Example:

```js
  {
    'name': 'verified_contracts',
    'datatypes': {
        'id': 'Int64',
        'created_at': 'datetime64[ns]',
        'updated_at': 'datetime64[ns]',
        'created_by': 'string',
        'updated_by': 'string',
        'deployment_id': 'string',
        'compilation_id': 'string',
        'creation_match': 'bool',
        'creation_values': 'string',
        'creation_transformations': 'string',
        'runtime_match': 'bool',
        'runtime_values': 'string',
        'runtime_transformations': 'string'
    },
    'chunk_size': 10000,
    'num_chunks_per_file': 10
  }
```

This config gives `10,000 * 10 = 100,000` rows per file.

The files will be named `verified_contracts_0_100000_zstd.parquet` and `verified_contracts_100000_200000_zstd.parquet` etc. (`zstd` is the compression algorithm).

The script also generates a `manifest.json` that contains a timestamp when the dump is created, and the list of files to access them in the S3 bucket.

```json
{
  "timestamp": 1718042395518,
  "dateStr": "2024-06-10T17:59:55.518972Z",
  "files": {
    "code": [
      "code/code_0_100000_zstd.parquet",
      "code/code_100000_200000_zstd.parquet",
      "code/code_200000_300000_zstd.parquet",
      "code/code_300000_400000_zstd.parquet",
      "code/code_400000_500000_zstd.parquet",
      "code/code_500000_600000_zstd.parquet",
      "code/code_600000_700000_zstd.parquet",
      "code/code_700000_800000_zstd.parquet"
    ],
    "contract_deployments": [...],
    "compiled_contracts": [...],
    "verified_contracts": [...]
  }
}
```

## Docker

Build the image:

```
docker build --tag=kuzdogan/test-parquet-linux --platform=linux/amd64 .
```

Publish:

```
docker push kuzdogan/test-parquet-linux
```
