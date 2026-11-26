import json
import os
import shutil

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
    "datatypes": {"id": "string", "value": "Int64"},
}
SOURCES = {
    "name": "sources",
    "primary_key": "source_hash",
    "order_by": "created_at",
    "datatypes": {"source_hash": "object", "content": "string"},
}
MATCHES = {
    "name": "sourcify_matches",
    "primary_key": "id",
    "order_by": "updated_at",
    "datatypes": {"id": "Int64", "value": "Int64"},
}
TABLES = [CONTRACTS, SOURCES, MATCHES]


def write_file(bucket, table, start, end, groups):
    """Writes one export file with one row group for each table of groups."""
    path = os.path.join(bucket.root, f"v2/{table}/{table}_{start}_{end}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = pq.ParquetWriter(path, groups[0].schema, compression=COMPRESSION)
    for group in groups:
        writer.write_table(group)
    writer.close()
    return path


def contracts(ids):
    return pa.table({"id": pa.array(ids, pa.string()), "value": pa.array(range(len(ids)), pa.int64())})


def matches(ids):
    return pa.table({"id": pa.array(ids, pa.int64()), "value": pa.array(range(len(ids)), pa.int64())})


def sources(hashes):
    return pa.table({"source_hash": pa.array(hashes, pa.binary()), "content": pa.array(["x"] * len(hashes))})


def removal_list(bucket, request_id, tables, prefix="parquet-removals"):
    bucket.write(
        f"{prefix}/pending/{request_id}.json",
        json.dumps({"version": 1, "requestId": request_id, "tables": tables}),
    )


def ids_of(path, column="id"):
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


def test_newest_file_is_by_row_range_not_by_time(buckets):
    export, _ = buckets
    write_file(export, "contracts", 10, 20, [contracts(["c"])])
    write_file(export, "contracts", 0, 10, [contracts(["a"])])
    write_file(export, "contracts", 100, 110, [contracts(["z"])])
    files = removal.list_table_files(export, "contracts")
    assert [start for start, _ in files] == [0, 10, 100]


def test_removes_rows_from_an_older_file_and_keeps_the_layout(buckets):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 4, [contracts(["a", "b"]), contracts(["c", "d"])])
    untouched = write_file(export, "contracts", 4, 8, [contracts(["e", "f"]), contracts(["g", "h"])])
    newest = write_file(export, "contracts", 8, 12, [contracts(["i", "b"])])
    removal_list(removals, "r1", {"contracts": ["b", "c"], "not_exported": ["1"]})

    assert run(buckets) == 2

    assert ids_of(old) == ["a", "d"]
    rewritten = pq.ParquetFile(old)
    assert rewritten.num_row_groups == 2
    assert rewritten.schema_arrow.equals(contracts([]).schema)
    assert rewritten.metadata.row_group(0).column(0).compression == "ZSTD"
    # Only the file with the rows is written again. The newest file is for the export.
    assert export.uploads == ["v2/contracts/contracts_0_4.parquet"]
    assert ids_of(untouched) == ["e", "f", "g", "h"]
    assert ids_of(newest) == ["i", "b"]
    assert removals.names() == ["parquet-removals/done/r1.json"]
    assert not [name for name in os.listdir(".") if name.startswith("removal_")]


def test_binary_keys_are_hex_in_the_list(buckets):
    export, removals = buckets
    old = write_file(export, "sources", 0, 2, [sources([b"\x01" * 32, b"\x02" * 32])])
    write_file(export, "sources", 2, 4, [sources([b"\x03" * 32])])
    removal_list(removals, "r1", {"sources": ["0x" + "02" * 32]})

    assert run(buckets) == 1
    assert ids_of(old, "source_hash") == [b"\x01" * 32]


def test_removes_each_copy_of_a_row_of_a_table_that_is_ordered_by_updated_at(buckets):
    export, removals = buckets
    first = write_file(export, "sourcify_matches", 0, 2, [matches([1, 2])])
    second = write_file(export, "sourcify_matches", 2, 4, [matches([2, 3])])
    write_file(export, "sourcify_matches", 4, 6, [matches([4])])
    removal_list(removals, "r1", {"sourcify_matches": ["2"]})

    assert run(buckets) == 2
    assert ids_of(first) == [1]
    assert ids_of(second) == [3]


def test_merges_lists_and_stops_when_each_unique_key_is_found(buckets):
    export, removals = buckets
    write_file(export, "contracts", 0, 2, [contracts(["a", "b"])])
    second = write_file(export, "contracts", 2, 4, [contracts(["c", "d"])])
    write_file(export, "contracts", 4, 6, [contracts(["e"])])
    removal_list(removals, "r1", {"contracts": ["a"]})
    removal_list(removals, "r2", {"contracts": ["b"]})
    # Not a Parquet file. The step must not read it, because both keys are in the first file.
    with open(second, "w") as file:
        file.write("not parquet")

    assert run(buckets) == 2
    assert removals.names() == ["parquet-removals/done/r1.json", "parquet-removals/done/r2.json"]


def test_a_key_that_no_older_file_has_is_not_an_error(buckets):
    export, removals = buckets
    write_file(export, "contracts", 0, 2, [contracts(["a", "b"])])
    write_file(export, "contracts", 2, 4, [contracts(["c"])])
    removal_list(removals, "r1", {"contracts": ["c", "missing"]})

    assert run(buckets) == 0
    assert export.uploads == []
    assert removals.names() == ["parquet-removals/done/r1.json"]


def test_dry_run_changes_nothing(buckets):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 2, [contracts(["a", "b"])])
    write_file(export, "contracts", 2, 4, [contracts(["c"])])
    removal_list(removals, "r1", {"contracts": ["a"]})

    assert run(buckets, dry_run=True) == 1
    assert ids_of(old) == ["a", "b"]
    assert removals.names() == ["parquet-removals/pending/r1.json"]


def test_a_failure_leaves_the_lists_pending(buckets, monkeypatch):
    export, removals = buckets
    old = write_file(export, "contracts", 0, 2, [contracts(["a", "b"])])
    write_file(export, "contracts", 2, 4, [contracts(["c"])])
    removal_list(removals, "r1", {"contracts": ["a"]})
    download = FakeBlob.download_to_filename

    def download_and_change(self, filename):
        download(self, filename)
        # Another writer changes the file after the download.
        export.generations[self.name] = 5

    monkeypatch.setattr(FakeBlob, "download_to_filename", download_and_change)
    with pytest.raises(RuntimeError):
        run(buckets)
    assert ids_of(old) == ["a", "b"]
    assert removals.names() == ["parquet-removals/pending/r1.json"]
    assert not [name for name in os.listdir(".") if name.startswith("removal_")]


def test_uses_the_prefix_and_refuses_an_unknown_list(buckets):
    export, removals = buckets
    write_file(export, "contracts", 0, 2, [contracts(["a"])])
    removal_list(removals, "r1", {"contracts": ["a"]}, prefix="parquet-removals/alliance")
    assert run(buckets) == 0
    assert removals.names() == ["parquet-removals/alliance/pending/r1.json"]
    assert run(buckets, prefix="parquet-removals/alliance") == 0
    assert removals.names() == ["parquet-removals/alliance/done/r1.json"]

    removals.write("parquet-removals/pending/bad.json", json.dumps({"version": 2}))
    with pytest.raises(ValueError):
        run(buckets)
