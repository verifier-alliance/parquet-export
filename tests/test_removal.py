import json
import os
import shutil
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import removal

COMPRESSION = "zstd"


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        # The generation at the time of the listing.
        self.generation = bucket.generations.get(name, 1)

    @property
    def path(self):
        return os.path.join(self.bucket.root, self.name)

    def open(self, mode, chunk_size=None):
        return open(self.path, mode)

    def download_as_bytes(self):
        with open(self.path, "rb") as file:
            return file.read()

    def download_to_filename(self, filename):
        shutil.copyfile(self.path, filename)

    def upload_from_filename(self, filename, if_generation_match=None):
        current = self.bucket.generations.get(self.name, 1)
        if if_generation_match is not None and if_generation_match != current:
            raise RuntimeError("generation mismatch")
        shutil.copyfile(filename, self.path)
        self.bucket.generations[self.name] = current + 1
        self.bucket.uploads.append(self.name)


class FakeBucket:
    """A bucket in a local directory, with the methods that removal.py uses."""

    def __init__(self, root):
        self.root = str(root)
        self.generations = {}
        self.uploads = []

    def names(self):
        found = []
        for directory, _, files in os.walk(self.root):
            for file in files:
                found.append(os.path.relpath(os.path.join(directory, file), self.root))
        return sorted(found)

    def list_blobs(self, prefix=""):
        return [FakeBlob(self, name) for name in self.names() if name.startswith(prefix)]

    def rename_blob(self, blob, new_name):
        target = os.path.join(self.root, new_name)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        os.rename(blob.path, target)

    def write(self, name, content):
        path = os.path.join(self.root, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as file:
            file.write(content)


CONTRACTS = {
    "name": "contracts",
    "primary_key": "id",
    "order_by": "created_at",
    "datatypes": {"id": "string", "created_at": "datetime64[ns]"},
}
SOURCES = {
    "name": "sources",
    "primary_key": "source_hash",
    "order_by": "created_at",
    "datatypes": {"source_hash": "object", "created_at": "datetime64[ns]"},
}
MATCHES = {
    "name": "sourcify_matches",
    "primary_key": "id",
    "order_by": "updated_at",
    "datatypes": {"id": "Int64", "created_at": "datetime64[ns]"},
}
TABLES = [CONTRACTS, SOURCES, MATCHES]
KEY_TYPES = {"contracts": pa.string(), "sources": pa.binary(), "sourcify_matches": pa.int64()}
H1, H2, H3, H4, H5 = (bytes([n]) * 32 for n in range(1, 6))


def created_at(day):
    """The createdAt text of a test row. The day is the only difference between rows."""
    return f"2026-01-{day:02d}T10:20:30.123456Z"


def rows(table, *pairs):
    """A row group of a test table from (key, day) pairs."""
    config = next(config for config in TABLES if config["name"] == table)
    times = [datetime(2026, 1, day, 10, 20, 30, 123456) for _, day in pairs]
    return pa.table(
        {
            config["primary_key"]: pa.array([key for key, _ in pairs], KEY_TYPES[table]),
            "created_at": pa.array(times, pa.timestamp("ns")),
        }
    )


def write_file(bucket, table, start, end, groups):
    """Writes one export file with one row group for each table of groups."""
    path = os.path.join(bucket.root, f"v2/{table}/{table}_{start}_{end}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = pq.ParquetWriter(path, groups[0].schema, compression=COMPRESSION)
    for group in groups:
        writer.write_table(group)
    writer.close()
    return path


def removal_list(bucket, request_id, tables, prefix="parquet-removals"):
    """tables: {table: [(key, day), ...]}"""
    content = {
        "version": 2,
        "requestId": request_id,
        "tables": {
            table: [{"key": key, "createdAt": created_at(day)} for key, day in pairs]
            for table, pairs in tables.items()
        },
    }
    bucket.write(f"{prefix}/pending/{request_id}.json", json.dumps(content))


def keys_of(path, column="id"):
    return pq.read_table(path).column(column).to_pylist()


@pytest.fixture
def buckets(tmp_path, monkeypatch):
    # removal.py writes its temporary files to the working directory.
    monkeypatch.chdir(tmp_path)
    return FakeBucket(tmp_path / "export"), FakeBucket(tmp_path / "removals")


def run(buckets, **kwargs):
    export, removals = buckets
    return removal.process_removals(export, removals, TABLES, COMPRESSION, **kwargs)


def test_parse_start_row():
    assert removal.parse_start_row("v2/contracts/contracts_2000000_3000000.parquet", "contracts") == 2000000
    assert removal.parse_start_row("v2/contracts/stats.json", "contracts") is None
    # A table name that is the start of another table name.
    assert removal.parse_start_row("v2/x/contracts_sources_0_10.parquet", "contracts") is None


def test_parse_created_at_keeps_the_microseconds():
    assert removal.parse_created_at("1970-01-01T00:00:01.000002Z") == 1000002
    assert removal.parse_created_at("2026-01-31T12:00:00.123456Z") % 1000000 == 123456


def test_newest_file_is_by_row_range_not_by_time(buckets):
    export, _ = buckets
    write_file(export, "contracts", 10, 20, [rows("contracts", ("c", 3))])
    write_file(export, "contracts", 0, 10, [rows("contracts", ("a", 1))])
    write_file(export, "contracts", 100, 110, [rows("contracts", ("z", 9))])
    files = removal.list_table_files(export, "contracts")
    assert [start for start, _ in files] == [0, 10, 100]


def test_removes_rows_from_an_older_file(buckets):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 4, [rows("contracts", ("a", 1), ("b", 2)), rows("contracts", ("c", 3), ("d", 4))])
    untouched = write_file(export, "contracts", 4, 8, [rows("contracts", ("e", 5), ("f", 6))])
    newest = write_file(export, "contracts", 8, 12, [rows("contracts", ("i", 9), ("j", 10))])
    removal_list(removals, "r1", {"contracts": [("b", 2), ("c", 3), ("j", 10)], "not_exported": [("1", 1)]})

    assert run(buckets) == 2

    assert keys_of(old) == ["a", "d"]
    rewritten = pq.ParquetFile(old)
    assert rewritten.num_row_groups == 2
    assert rewritten.schema_arrow.equals(rows("contracts").schema)
    assert rewritten.metadata.row_group(0).column(0).compression == "ZSTD"
    # Only the file with the rows is written again. The newest file is for the export.
    assert export.uploads == ["v2/contracts/contracts_0_4.parquet"]
    assert keys_of(untouched) == ["e", "f"]
    assert keys_of(newest) == ["i", "j"]
    assert removals.names() == ["parquet-removals/done/r1.json"]
    assert not [name for name in os.listdir(".") if name.startswith("removal_")]


def test_a_row_group_that_loses_all_rows_is_left_out(buckets):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 3, [rows("contracts", ("a", 1)), rows("contracts", ("b", 2), ("c", 3))])
    write_file(export, "contracts", 3, 6, [rows("contracts", ("d", 4))])
    removal_list(removals, "r1", {"contracts": [("a", 1)]})

    assert run(buckets) == 1
    assert keys_of(old) == ["b", "c"]
    assert pq.ParquetFile(old).num_row_groups == 1


def test_a_row_with_the_same_key_and_another_time_stays(buckets):
    """The same content was added again after the deletion. The new row is in the database."""
    export, removals = buckets
    first = write_file(export, "sources", 0, 2, [rows("sources", (H1, 1), (H2, 2))])
    write_file(export, "sources", 2, 4, [rows("sources", (H3, 3), (H4, 4))])
    again = write_file(export, "sources", 4, 6, [rows("sources", (H5, 5), (H2, 6))])
    write_file(export, "sources", 6, 8, [rows("sources", (b"\x09" * 32, 9))])
    # The second key is in no older file, so the step cannot stop early.
    removal_list(removals, "r1", {"sources": [("0x" + H2.hex(), 2), ("0x" + "09" * 32, 9)]})

    assert run(buckets) == 1
    assert keys_of(first, "source_hash") == [H1]
    assert keys_of(again, "source_hash") == [H5, H2]
    assert export.uploads == ["v2/sources/sources_0_2.parquet"]


def test_removes_each_copy_of_a_row_of_a_table_that_is_ordered_by_updated_at(buckets):
    export, removals = buckets
    first = write_file(export, "sourcify_matches", 0, 2, [rows("sourcify_matches", (1, 1), (2, 2))])
    second = write_file(export, "sourcify_matches", 2, 4, [rows("sourcify_matches", (2, 2), (3, 3))])
    write_file(export, "sourcify_matches", 4, 6, [rows("sourcify_matches", (4, 4))])
    removal_list(removals, "r1", {"sourcify_matches": [("2", 2)]})

    assert run(buckets) == 2
    assert keys_of(first) == [1]
    assert keys_of(second) == [3]


def test_reads_only_row_groups_that_can_have_a_row(buckets, monkeypatch):
    export, removals = buckets
    write_file(export, "contracts", 0, 4, [rows("contracts", ("a", 1), ("b", 2)), rows("contracts", ("c", 3), ("d", 4))])
    write_file(export, "contracts", 4, 6, [rows("contracts", ("e", 5), ("f", 6))])
    write_file(export, "contracts", 6, 8, [rows("contracts", ("g", 7))])
    removal_list(removals, "r1", {"contracts": [("d", 4)]})
    read = []
    original = pq.ParquetFile.read_row_groups

    def record(self, row_groups, **kwargs):
        read.append((list(row_groups), kwargs.get("columns")))
        return original(self, row_groups, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "read_row_groups", record)
    assert run(buckets) == 1
    # One row group of the first file, two columns. The second file is not opened.
    assert read == [([1], ["id", "created_at"])]


def test_merges_lists(buckets):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 2, [rows("contracts", ("a", 1), ("b", 2))])
    write_file(export, "contracts", 2, 4, [rows("contracts", ("c", 3))])
    removal_list(removals, "r1", {"contracts": [("a", 1)]})
    removal_list(removals, "r2", {"contracts": [("b", 2)]})

    assert run(buckets) == 2
    assert keys_of(old) == []
    assert pq.ParquetFile(old).schema_arrow.equals(rows("contracts").schema)
    assert removals.names() == ["parquet-removals/done/r1.json", "parquet-removals/done/r2.json"]


def test_a_row_that_no_older_file_has_is_not_an_error(buckets):
    export, removals = buckets
    write_file(export, "contracts", 0, 2, [rows("contracts", ("a", 1), ("b", 2))])
    write_file(export, "contracts", 2, 4, [rows("contracts", ("c", 3))])
    removal_list(removals, "r1", {"contracts": [("c", 3), ("missing", 2)]})

    assert run(buckets) == 0
    assert export.uploads == []
    assert removals.names() == ["parquet-removals/done/r1.json"]


def test_dry_run_changes_nothing(buckets):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 2, [rows("contracts", ("a", 1), ("b", 2))])
    write_file(export, "contracts", 2, 4, [rows("contracts", ("c", 3))])
    removal_list(removals, "r1", {"contracts": [("a", 1)]})

    assert run(buckets, dry_run=True) == 1
    assert keys_of(old) == ["a", "b"]
    assert removals.names() == ["parquet-removals/pending/r1.json"]


def test_a_failure_leaves_the_lists_pending(buckets, monkeypatch):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 2, [rows("contracts", ("a", 1), ("b", 2))])
    write_file(export, "contracts", 2, 4, [rows("contracts", ("c", 3))])
    removal_list(removals, "r1", {"contracts": [("a", 1)]})
    download = FakeBlob.download_to_filename

    def download_and_change(self, filename):
        download(self, filename)
        # Another writer changes the file after the download.
        export.generations[self.name] = 5

    monkeypatch.setattr(FakeBlob, "download_to_filename", download_and_change)
    with pytest.raises(RuntimeError):
        run(buckets)
    assert keys_of(old) == ["a", "b"]
    assert removals.names() == ["parquet-removals/pending/r1.json"]
    assert not [name for name in os.listdir(".") if name.startswith("removal_")]


def test_uses_the_prefix(buckets):
    export, removals = buckets
    write_file(export, "contracts", 0, 2, [rows("contracts", ("a", 1))])
    removal_list(removals, "r1", {"contracts": [("a", 1)]}, prefix="parquet-removals/alliance")
    assert run(buckets) == 0
    assert removals.names() == ["parquet-removals/alliance/pending/r1.json"]
    assert run(buckets, prefix="parquet-removals/alliance") == 0
    assert removals.names() == ["parquet-removals/alliance/done/r1.json"]


@pytest.mark.parametrize(
    "content",
    [
        {"version": 1, "tables": {"contracts": ["a"]}},
        {"version": 2},
        {"version": 2, "tables": []},
        # One string in place of the array. Its characters must not become keys.
        {"version": 2, "tables": {"sourcify_matches": "2345"}},
        {"version": 2, "tables": {"sourcify_matches": {"2": 1}}},
        {"version": 2, "tables": {"contracts": ["a"]}},
        {"version": 2, "tables": {"contracts": [{"key": "a"}]}},
        {"version": 2, "tables": {"contracts": [{"key": 1, "createdAt": "2026-01-01T10:20:30.123456Z"}]}},
        {"version": 2, "tables": {"contracts": [{"key": "a", "createdAt": "2026-01-01T10:20:30Z"}]}},
        {"version": 2, "tables": {"contracts": [{"key": "a", "createdAt": "2026-01-01T10:20:30.123456+01:00"}]}},
        [],
    ],
)
def test_refuses_a_list_with_another_format_and_removes_nothing(buckets, content):
    export, removals = buckets
    old = write_file(export, "sourcify_matches", 0, 6, [rows("sourcify_matches", *[(n, n) for n in range(1, 7)])])
    write_file(export, "sourcify_matches", 6, 8, [rows("sourcify_matches", (7, 7))])
    one = write_file(export, "contracts", 0, 2, [rows("contracts", ("a", 1), ("b", 2))])
    write_file(export, "contracts", 2, 4, [rows("contracts", ("c", 3))])
    # A valid list next to the wrong list. The step handles all lists or no list.
    removal_list(removals, "good", {"contracts": [("a", 1)]})
    removals.write("parquet-removals/pending/wrong.json", json.dumps(content))

    with pytest.raises(ValueError):
        run(buckets)
    assert keys_of(old) == [1, 2, 3, 4, 5, 6]
    assert keys_of(one) == ["a", "b"]
    assert sorted(removals.names()) == ["parquet-removals/pending/good.json", "parquet-removals/pending/wrong.json"]
