"""
Removes rows from Parquet files that are already exported.

The export is append-only: only the newest file of a table is written again
from the database. So a row that was deleted in the database stays in an
older file. A removal list names such rows. This module writes the affected
older files again without them.

A removal list is a JSON file in a private bucket:

    {"version": 1, "requestId": "...", "tables": {"<table>": ["<key>", ...]}}

A key is the primary key of the table (see config.py): integers and UUIDs as
text, binary values as 0x hex.
"""
import bisect
import json
import logging
import os
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

logger = logging.getLogger()

DEFAULT_REMOVAL_PREFIX = "parquet-removals"
EXPORT_PREFIX = "v2"
# Size of one ranged read of a remote file.
REMOTE_CHUNK_SIZE = 8 * 1024 * 1024


def parse_start_row(blob_name, table_name):
    """Gives the start row of <table>_<start>_<end>.parquet, or None for another name."""
    file_name = blob_name.split("/")[-1]
    match = re.fullmatch(re.escape(table_name) + r"_(\d+)_(\d+)\.parquet", file_name)
    return int(match.group(1)) if match else None


def list_table_files(bucket, table_name):
    """Gives the export files of a table as (start_row, blob), sorted by start row."""
    files = []
    for blob in bucket.list_blobs(prefix=f"{EXPORT_PREFIX}/{table_name}/"):
        start_row = parse_start_row(blob.name, table_name)
        if start_row is not None:
            files.append((start_row, blob))
    return sorted(files, key=lambda file: file[0])


def convert_keys(keys, dtype):
    """Converts the keys of a removal list to the values of the Parquet column."""
    if dtype in ("Int64", "Int32"):
        return sorted({int(key) for key in keys})
    if dtype == "string":
        return sorted({str(key) for key in keys})
    if dtype == "object":
        return sorted({bytes.fromhex(key[2:] if key.startswith("0x") else key) for key in keys})
    raise ValueError(f"Keys of type {dtype} are not supported")


def read_removal_lists(bucket, prefix):
    """Gives (blobs, {table: [keys]}) of all pending removal lists."""
    blobs = [
        blob
        for blob in bucket.list_blobs(prefix=f"{prefix}/pending/")
        if blob.name.endswith(".json")
    ]
    tables = {}
    for blob in blobs:
        content = json.loads(blob.download_as_bytes())
        if content.get("version") != 1 or not isinstance(content.get("tables"), dict):
            raise ValueError(f"{blob.name} is not a removal list of version 1")
        for table, keys in content["tables"].items():
            tables.setdefault(table, []).extend(keys)
    return blobs, tables


def any_key_in_range(sorted_keys, minimum, maximum):
    index = bisect.bisect_left(sorted_keys, minimum)
    return index < len(sorted_keys) and sorted_keys[index] <= maximum


def candidate_row_groups(parquet_file, column, sorted_keys):
    """Gives the row groups that can have a key, by the min and max of the column."""
    column_index = parquet_file.schema_arrow.get_field_index(column)
    groups = []
    for index in range(parquet_file.num_row_groups):
        stats = parquet_file.metadata.row_group(index).column(column_index).statistics
        if stats is not None and stats.has_min_max:
            try:
                if not any_key_in_range(sorted_keys, stats.min, stats.max):
                    continue
            except TypeError:
                # The statistics have another type. Read the row group.
                pass
        groups.append(index)
    return groups


def find_keys(parquet_file, column, sorted_keys):
    """Gives the keys of the list that the file has. Reads only the key column."""
    groups = candidate_row_groups(parquet_file, column, sorted_keys)
    if not groups:
        return set()
    values = parquet_file.read_row_groups(groups, columns=[column]).column(column)
    value_set = pa.array(sorted_keys, type=values.type)
    found = pc.filter(values, pc.is_in(values, value_set=value_set))
    return set(found.to_pylist())


def rewrite_without_keys(source_path, target_path, column, sorted_keys, compression):
    """Writes the file again without the rows of the keys. Gives the number of removed rows."""
    source = pq.ParquetFile(source_path)
    value_set = pa.array(sorted_keys, type=source.schema_arrow.field(column).type)
    removed = 0
    writer = pq.ParquetWriter(target_path, source.schema_arrow, compression=compression)
    try:
        # One row group in, one row group out. So the layout of the file stays.
        for index in range(source.num_row_groups):
            group = source.read_row_group(index)
            kept = group.filter(pc.invert(pc.is_in(group.column(column), value_set=value_set)))
            removed += group.num_rows - kept.num_rows
            if kept.num_rows > 0:
                writer.write_table(kept)
    finally:
        writer.close()
    return removed


def remove_from_table(bucket, table_config, keys, compression, dry_run=False):
    """
    Removes the rows of the keys from the older files of a table.
    The newest file is skipped: the export writes it again from the database.
    Gives the number of removed rows.
    """
    table_name = table_config["name"]
    column = table_config["primary_key"]
    sorted_keys = convert_keys(keys, table_config["datatypes"][column])
    # Only a table that is ordered by created_at has each row one time.
    rows_are_unique = table_config["order_by"] == "created_at"
    files = list_table_files(bucket, table_name)
    older_files = files[:-1]
    logger.info(f"Removal: {table_name}: {len(sorted_keys)} keys, {len(older_files)} older files")

    pending = set(sorted_keys)
    removed_total = 0
    for _, blob in older_files:
        if rows_are_unique and not pending:
            break
        with blob.open("rb", chunk_size=REMOTE_CHUNK_SIZE) as remote:
            found = find_keys(pq.ParquetFile(remote), column, sorted_keys)
        if not found:
            continue
        pending -= found

        file_name = blob.name.split("/")[-1]
        source_path = f"removal_source_{file_name}"
        target_path = f"removal_target_{file_name}"
        try:
            blob.download_to_filename(source_path)
            removed = rewrite_without_keys(source_path, target_path, column, sorted_keys, compression)
            removed_total += removed
            if dry_run:
                logger.info(f"Removal: DEBUG: NOT uploading {blob.name} ({removed} rows removed)")
            else:
                # Fail if another writer changed the file after the download.
                blob.upload_from_filename(target_path, if_generation_match=blob.generation)
                logger.info(f"Removal: wrote {blob.name} again without {removed} rows")
        finally:
            for path in (source_path, target_path):
                if os.path.exists(path):
                    os.remove(path)

    logger.info(f"Removal: {table_name}: removed {removed_total} rows from older files")
    return removed_total


def process_removals(export_bucket, removal_bucket, tables_config, compression, prefix=DEFAULT_REMOVAL_PREFIX, dry_run=False):
    """
    Handles all pending removal lists. Run it before the export, so that the
    export then writes the newest file of each table again.
    Gives the number of removed rows.
    """
    blobs, tables = read_removal_lists(removal_bucket, prefix)
    if not blobs:
        logger.info(f"Removal: no pending list in {prefix}/pending/")
        return 0
    logger.info(f"Removal: {len(blobs)} pending lists")

    configs = {config["name"]: config for config in tables_config}
    removed_total = 0
    for table, keys in tables.items():
        if table not in configs:
            logger.info(f"Removal: {table} is not in the export. Skipped.")
            continue
        if keys:
            removed_total += remove_from_table(export_bucket, configs[table], keys, compression, dry_run)

    if not dry_run:
        # Only after all tables are done. A failed run handles the lists again.
        for blob in blobs:
            done_name = blob.name.replace(f"{prefix}/pending/", f"{prefix}/done/", 1)
            removal_bucket.rename_blob(blob, done_name)
            logger.info(f"Removal: moved {blob.name} to {done_name}")
    return removed_total
