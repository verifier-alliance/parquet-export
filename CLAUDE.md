# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based data export tool that exports the Verifier Alliance PostgreSQL database to Parquet format and uploads the files to Google Cloud Storage. The exported data is publicly available at https://export.verifieralliance.org.

## Development Setup

### Local Environment

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env-template .env
# Edit .env with your database and GCS credentials
```

### Running the Export

```bash
# Full export
python main.py

# Debug specific table only
DEBUG_TABLE=verified_contracts python main.py

# Debug with offset and reduced chunk size
DEBUG=1 DEBUG_TABLE=code DEBUG_OFFSET=1000 python main.py
```

## Architecture

### Core Components

- **[main.py](main.py)**: Entry point and orchestration logic

  - Database connection handling (local TCP or Google Cloud SQL Connector)
  - Chunked data fetching from PostgreSQL using SQLAlchemy streaming
  - Parquet file writing with PyArrow
  - GCS upload via google-cloud-storage
  - Manifest generation

- **[config.py](config.py)**: Table configuration defining:
  - Column datatypes for each database table
  - Chunk sizes (rows per chunk)
  - Number of chunks per file
  - Custom datatype handling (bytes as 'object', JSON preservation)

### Data Flow

1. **Fetch**: Stream data from PostgreSQL in configurable chunks
2. **Process**: Apply dtype conversions, handle memoryview→bytes, preserve JSON formatting
3. **Convert**: Transform pandas DataFrame to PyArrow Table with explicit schema
4. **Write**: Write chunks to Parquet files with zstd compression
5. **Upload**: Upload completed files to GCS and delete local copies
6. **Manifest**: Generate manifest.json with timestamps and file lists

### Database Connection Strategy

The script automatically detects the environment:

- **Google Cloud**: Uses Cloud SQL Connector (pg8000) when `GOOGLE_CLOUD_PROJECT`, `CLOUD_RUN_JOB`, or `K_SERVICE` env vars are present
- **Local/TCP**: Uses standard PostgreSQL connection via SQLAlchemy when running locally

### Table Configuration

Each table in `tables_config` specifies:

- `chunk_size`: Rows fetched per database query
- `num_chunks_per_file`: Chunks combined into each Parquet file
- `datatypes`: Explicit column type mappings (Int64, string, bool, datetime64[ns], object, json)

Files are named: `{table}_{start_row}_{end_row}_zstd.parquet`

Example: `verified_contracts_0_100000_zstd.parquet` contains rows 0-99,999

### Special Datatype Handling

- **'json'**: Not a pandas dtype - preserved as JSON strings (not Python object notation) then converted to 'string'
- **'object'**: Used for binary data (bytea columns like code_hash, address)
- **memoryview**: Automatically converted to bytes via `convert_memoryview_to_bytes()`

### Environment Variables

Required (see [.env-template](.env-template)):

- `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`: Database connection
- `DB_SCHEMA`: PostgreSQL schema name (typically 'public')
- `GCS_BUCKET_NAME`: Google Cloud Storage bucket name
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account key JSON (for local development)

Optional for debugging:

- `DEBUG`: Enables debug logging, reduces chunk sizes by 100x, processes only 1 file per table, skips GCS upload
- `DEBUG_TABLE`: Process only this specific table
- `DEBUG_OFFSET`: Add OFFSET to SQL queries

Google Cloud (auto-detected):

- `INSTANCE_CONNECTION_NAME`: GCP Cloud SQL instance connection string
- In Cloud Run, authentication is automatic via Workload Identity

## Deployment

### GitHub Actions

Automatic deployment on push to `main` or `staging` branches:

- Builds Docker image and pushes to GitHub Container Registry (ghcr.io)
- Deploys to Google Cloud Run Job using Workload Identity Federation
- Separate jobs for production (`main`) and staging (`staging`)

See [.github/workflows/release.yml](.github/workflows/release.yml)

## Database Schema

The script exports these tables from the Verifier Alliance database:

- `code`: Bytecode with multiple hash types
- `contracts`: Contract definitions with creation/runtime code hashes
- `contract_deployments`: On-chain deployment records (chain_id, address, transaction details)
- `compiled_contracts`: Compiler metadata and artifacts (JSON fields)
- `compiled_contracts_sources`: Join table linking compilations to source files
- `sources`: Source code content with hash identifiers
- `verified_contracts`: Verification results with match types and transformations

All tables include audit fields: `created_at`, `updated_at`, `created_by`, `updated_by`

## Important Notes

- When modifying table configs, ensure datatypes match the PostgreSQL schema
- The script uses streaming to handle large tables without loading everything into memory
- Files are uploaded and deleted immediately after creation to minimize disk usage
- The manifest.json is generated last and contains metadata about all uploaded files
- In DEBUG mode, GCS uploads are skipped and chunk sizes are reduced 100x
- Authentication to GCS uses Application Default Credentials (service account in Cloud Run, GOOGLE_APPLICATION_CREDENTIALS locally)
