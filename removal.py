"""
Removes rows from Parquet files that are already exported.

The export is append-only: only the newest file of a table is written again
from the database. So a row that was deleted in the database stays in an
older file. A removal list names such rows. This module writes the affected
older files again without them.

A removal list is a JSON file in a private bucket:

    {"version": 2, "requestId": "...", "tables": {"<table>": [
        {"key": "<key>", "createdAt": "2026-01-31T12:00:00.123456Z"}, ...]}}

key is the primary key of the row (see config.py): integers and UUIDs as
text, binary values as 0x hex. createdAt is the created_at value of the row
in UTC with microseconds. A row is removed only if both values match: a
content hash comes back when the same content is added again, and that new
row must stay.
"""
import bisect
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

logger = logging.getLogger()

DEFAULT_REMOVAL_PREFIX = "parquet-removals"
EXPORT_PREFIX = "v2"
CREATED_AT = "created_at"
# Size of one ranged read of a remote file. A footer and a key column are small.
REMOTE_CHUNK_SIZE = 256 * 1024

_CREATED_AT_FORMAT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


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


def convert_key(key, dtype):
    """Converts a key of a removal list to the value of the Parquet column."""
    if dtype in ("Int64", "Int32"):
        return int(key)
    if dtype == "string":
        return key
    if dtype == "object":
        return bytes.fromhex(key[2:] if key.startswith("0x") else key)
    raise ValueError(f"Keys of type {dtype} are not supported")


def parse_created_at(text):
    """Gives the microseconds since 1970 of a createdAt value."""
    moment = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return (moment - _EPOCH) // timedelta(microseconds=1)


def read_removal_lists(bucket, prefix):
    """
    Gives (blobs, {table: [(key, created_at_microseconds), ...]}) of all pending lists.
    Fails on a list with another format. A wrong list can remove rows that must stay.
    """
    blobs = [
        blob
        for blob in bucket.list_blobs(prefix=f"{prefix}/pending/")
        if blob.name.endswith(".json")
    ]
    tables = {}
    for blob in blobs:
        content = json.loads(blob.download_as_bytes())
        if not isinstance(content, dict) or content.get("version") != 2 or not isinstance(content.get("tables"), dict):
            raise ValueError(f"{blob.name} is not a removal list of version 2")
        for table, rows in content["tables"].items():
            if not isinstance(rows, list):
                raise ValueError(f"{blob.name}: {table} is not an array")
            for row in rows:
                if (
                    not isinstance(row, dict)
                    or not isinstance(row.get("key"), str)
                    or row["key"] == ""
                    or not isinstance(row.get("createdAt"), str)
                    or not _CREATED_AT_FORMAT.fullmatch(row["createdAt"])
                ):
                    raise ValueError(f"{blob.name}: {table} has a row without a valid key and createdAt")
                tables.setdefault(table, []).append((row["key"], parse_created_at(row["createdAt"])))
    return blobs, tables


def ticks_per_microsecond(parquet_file):
    """Gives the factor from microseconds to the unit of the created_at column."""
    column_type = parquet_file.schema_arrow.field(CREATED_AT).type
    if not pa.types.is_timestamp(column_type) or column_type.unit not in ("ns", "us"):
        raise ValueError(f"The {CREATED_AT} column has the type {column_type}, not a timestamp in ns or us")
    return 1000 if column_type.unit == "ns" else 1


def candidate_row_groups(parquet_file, sorted_times):
    """Gives the row groups that can have a wanted row, by the min and max of created_at."""
    column_index = parquet_file.schema_arrow.get_field_index(CREATED_AT)
    factor = ticks_per_microsecond(parquet_file)
    groups = []
    for index in range(parquet_file.num_row_groups):
        stats = parquet_file.metadata.row_group(index).column(column_index).statistics
        if stats is not None and stats.has_min_max:
            # The raw values are integers in the unit of the column.
            first = bisect.bisect_left(sorted_times, stats.min_raw // factor)
            if first == len(sorted_times) or sorted_times[first] * factor > stats.max_raw:
                continue
        groups.append(index)
    return groups


def matching_rows(table, column, wanted, factor):
    """Gives a list with True for each row of the table that is in wanted."""
    keys = table.column(column)
    is_candidate = pc.is_in(keys, value_set=pa.array(list(wanted), type=keys.type))
    matches = [False] * table.num_rows
    indices = pc.indices_nonzero(is_candidate).to_pylist()
    if indices:
        times = table.column(CREATED_AT).cast(pa.int64())
        for index in indices:
            if times[index].as_py() in {time * factor for time in wanted[keys[index].as_py()]}:
                matches[index] = True
    return matches


def find_rows(parquet_file, column, wanted, sorted_times):
    """
    Gives the (key, created_at_microseconds) pairs of wanted that the file has.
    Reads the footer, and the key and created_at columns of the row groups that can have a row.
    """
    groups = candidate_row_groups(parquet_file, sorted_times)
    if not groups:
        return set()
    factor = ticks_per_microsecond(parquet_file)
    table = parquet_file.read_row_groups(groups, columns=[column, CREATED_AT])
    keys = table.column(column)
    times = table.column(CREATED_AT).cast(pa.int64())
    return {
        (keys[index].as_py(), times[index].as_py() // factor)
        for index, match in enumerate(matching_rows(table, column, wanted, factor))
        if match
    }


def rewrite_without_rows(source_path, target_path, column, wanted, compression):
    """Writes the file again without the wanted rows. Gives the number of removed rows."""
    source = pq.ParquetFile(source_path)
    factor = ticks_per_microsecond(source)
    removed = 0
    writer = pq.ParquetWriter(target_path, source.schema_arrow, compression=compression)
    try:
        # One row group in, one row group out. A row group that loses all rows is left out.
        for index in range(source.num_row_groups):
            group = source.read_row_group(index)
            matches = matching_rows(group, column, wanted, factor)
            if any(matches):
                kept = group.filter(pa.array([not match for match in matches]))
                removed += group.num_rows - kept.num_rows
                group = kept
            if group.num_rows > 0:
                writer.write_table(group)
    finally:
        writer.close()
    return removed


def remove_from_table(bucket, table_config, rows, compression, dry_run=False):
    """
    Removes the rows from the older files of a table. rows is a list of
    (key, created_at_microseconds). The newest file is skipped: the export
    writes it again from the database. Gives the number of removed rows.
    """
    table_name = table_config["name"]
    column = table_config["primary_key"]
    dtype = table_config["datatypes"][column]
    wanted = {}
    for key, created_at in rows:
        wanted.setdefault(convert_key(key, dtype), set()).add(created_at)
    sorted_times = sorted({time for times in wanted.values() for time in times})
    # Only a table that is ordered by created_at has each row one time.
    rows_are_unique = table_config["order_by"] == CREATED_AT
    files = list_table_files(bucket, table_name)
    older_files = files[:-1]
    logger.info(f"Removal: {table_name}: {len(rows)} rows, {len(older_files)} older files")

    pending = {(key, time) for key, times in wanted.items() for time in times}
    removed_total = 0
    for _, blob in older_files:
        if rows_are_unique and not pending:
            break
        with blob.open("rb", chunk_size=REMOTE_CHUNK_SIZE) as remote:
            found = find_rows(pq.ParquetFile(remote), column, wanted, sorted_times)
        if not found:
            continue
        pending -= found

        file_name = blob.name.split("/")[-1]
        source_path = f"removal_source_{file_name}"
        target_path = f"removal_target_{file_name}"
        try:
            blob.download_to_filename(source_path)
            removed = rewrite_without_rows(source_path, target_path, column, wanted, compression)
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
    Handles all pending removal lists. Run it after the export: the export
    wrote the newest file of each table again from the database, so only the
    older files can still have a deleted row. An error stops the job, and the
    lists stay pending for the next run.
    Gives the number of removed rows.
    """
    blobs, tables = read_removal_lists(removal_bucket, prefix)
    if not blobs:
        logger.info(f"Removal: no pending list in {prefix}/pending/")
        return 0
    logger.info(f"Removal: {len(blobs)} pending lists")

    configs = {config["name"]: config for config in tables_config}
    removed_total = 0
    for table, rows in tables.items():
        if table not in configs:
            logger.info(f"Removal: {table} is not in the export. Skipped.")
            continue
        if rows:
            removed_total += remove_from_table(export_bucket, configs[table], rows, compression, dry_run)

    if not dry_run:
        # Only after all tables are done. A failed run handles the lists again.
        for blob in blobs:
            done_name = blob.name.replace(f"{prefix}/pending/", f"{prefix}/done/", 1)
            removal_bucket.rename_blob(blob, done_name)
            logger.info(f"Removal: moved {blob.name} to {done_name}")
    return removed_total
