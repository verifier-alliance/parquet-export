# Conversation Export: Migration from S3 to GCS with Direct Streaming

## Date
2025-10-23

## Summary
This conversation covered two main topics:
1. Reviewing the migration from AWS S3 to Google Cloud Storage (GCS)
2. Implementing direct streaming to GCS to eliminate local filesystem usage

---

## Part 1: Code Review - S3 to GCS Migration

### User Request
"can you review the changes currently not commited"

### Changes Reviewed

#### Files Modified:
1. **`.env-template`**: Removed AWS S3 variables, added GCS variables
2. **`main.py`**: Replaced boto3 with google-cloud-storage library
3. **`requirements.txt`**: Removed boto3/botocore, added google-cloud-storage
4. **`CLAUDE.md`**: New documentation file for the project

#### Review Findings

**✅ Positives:**
- Clean migration from boto3 to google-cloud-storage
- Consistent renaming of functions (`upload_to_s3` → `upload_to_gcs`)
- Proper error handling with generic Exception catching
- Good logging throughout
- Application Default Credentials for authentication (cleaner than explicit keys)
- Comprehensive documentation in CLAUDE.md

**⚠️ Issues Found:**
1. CLAUDE.md line 23 still referenced "S3 credentials" instead of "GCS credentials"
2. Missing trailing newlines at end of files (minor style issue)
3. README.md still had outdated references to S3 and Cloudflare R2

### Actions Taken

**Fixed CLAUDE.md** (line 23):
```bash
# Edit .env with your database and GCS credentials
```

**Updated README.md:**
- Changed "S3 bucket" → "Google Cloud Storage" (line 3)
- Changed "Cloudflare R2" → "publicly available at" (line 5)
- Added documentation for `DEBUG` environment variable (line 41)
- Added new "Authentication" section explaining:
  - `GOOGLE_APPLICATION_CREDENTIALS` for local development
  - Workload Identity for Cloud Run
- Changed "S3 bucket" → "Google Cloud Storage" in manifest description (line 88)

---

## Part 2: Implementing Direct GCS Streaming

### User Request
"is there a way to write the data directly to the bucket and not store on the filesystem first"

### Solution Approach
Implement streaming writes directly to GCS using PyArrow's support for file-like objects combined with GCS blob's `open('wb')` method.

### Implementation Details

#### 1. New Function: `create_parquet_writer()` (lines 179-213)

**Purpose:** Create a ParquetWriter that streams directly to GCS

**Signature:**
```python
def create_parquet_writer(bucket_name, object_name, schema, compression):
    """
    Returns:
        tuple: (writer, local_file_path)
            - writer: PyArrow ParquetWriter object
            - local_file_path: Path to local file if in DEBUG mode, None otherwise
    """
```

**Behavior:**
- **DEBUG mode**: Creates local file writer (original behavior for testing)
- **Production mode**:
  - Opens GCS blob as writable file-like object
  - Creates ParquetWriter with the blob file handle
  - Data streams directly to GCS without touching disk

**Code:**
```python
if os.getenv("DEBUG"):
    # In debug mode, write to local filesystem
    local_file = object_name.split('/')[-1]
    logger.debug(f"DEBUG: Creating local writer for {local_file}")
    writer = pq.ParquetWriter(local_file, schema, compression=compression)
    return writer, local_file

# Production mode: stream directly to GCS
client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob(object_name)
gcs_file = blob.open('wb')
writer = pq.ParquetWriter(gcs_file, schema, compression=compression)
return writer, None
```

#### 2. Updated: `fetch_and_write()` (lines 215-350)

**Changes:**
- Added tracking variables: `local_file`, `object_name`
- Replaced direct `pq.ParquetWriter()` calls with `create_parquet_writer()`
- Updated file completion logic:

**Before:**
```python
writer.close()
logger.info(f"Written {output_file}")
upload_to_gcs(output_file, os.getenv('GCS_BUCKET_NAME'), object_name)
```

**After:**
```python
writer.close()

if local_file:
    # DEBUG mode: upload the local file
    logger.info(f"Written local file {local_file}")
    upload_to_gcs(local_file, os.getenv('GCS_BUCKET_NAME'), object_name)
else:
    # Production mode: already streamed to GCS
    logger.info(f"Streamed directly to gs://{os.getenv('GCS_BUCKET_NAME')}/{object_name}")
```

#### 3. New Function: `write_manifest_to_gcs()` (lines 115-146)

**Purpose:** Write manifest.json directly to GCS without local file

**Code:**
```python
def write_manifest_to_gcs():
    """Write manifest.json directly to GCS without local file (or locally in DEBUG mode)"""
    if os.getenv("DEBUG"):
        write_manifest()
        logger.debug("DEBUG: Manifest written locally, skipping GCS upload")
        return

    timestamp = int(datetime.now().timestamp() * 1000)
    date_str = datetime.now().isoformat() + "Z"
    manifest = {
        "timestamp": timestamp,
        "dateStr": date_str,
        "files": uploaded_files
    }

    client = storage.Client()
    bucket = client.bucket(os.getenv('GCS_BUCKET_NAME'))
    blob = bucket.blob('manifest.json')
    blob.upload_from_string(
        json.dumps(manifest, indent=2),
        content_type='application/json'
    )
    logger.info(f"Manifest written directly to gs://{os.getenv('GCS_BUCKET_NAME')}/manifest.json")
```

#### 4. Updated: Main Execution (lines 369-370)

**Before:**
```python
write_manifest()
upload_to_gcs('manifest.json', os.getenv('GCS_BUCKET_NAME'), 'manifest.json')
```

**After:**
```python
write_manifest_to_gcs()
```

### Benefits of Streaming Implementation

1. **Zero local disk usage in production**
   - No temporary Parquet files on filesystem
   - Important for large exports that could fill disk
   - Eliminates need for disk space monitoring

2. **Faster execution**
   - Eliminates write → upload → delete cycle
   - Single operation instead of three

3. **Simpler cleanup**
   - No orphaned files if process crashes mid-export
   - No need for cleanup scripts

4. **Better resource utilization**
   - Memory → Network directly
   - No disk I/O bottleneck

5. **Backward compatible**
   - DEBUG mode still uses local files for easy inspection
   - Existing debugging workflows unchanged

### Data Flow Comparison

**Before (with local files):**
```
Database → Pandas → PyArrow → Local File → GCS Upload → Delete Local File
           ↓         ↓          ↓            ↓            ↓
         Network   Memory    Disk I/O    Network I/O   Disk I/O
```

**After (direct streaming):**
```
Database → Pandas → PyArrow → GCS Blob (direct)
           ↓         ↓          ↓
         Network   Memory    Network I/O
```

**DEBUG Mode (unchanged):**
```
Database → Pandas → PyArrow → Local File (+ optional upload)
```

### Testing Verification

**Syntax Check:** ✅ Passed
```bash
python -m py_compile main.py
# No errors
```

**Expected Log Messages:**

Production mode:
```
Creating streaming writer to gs://bucket/table/file.parquet
Writing chunk 0 of file 0
Writing chunk 1 of file 0
...
Streamed directly to gs://bucket/table/file.parquet
Manifest written directly to gs://bucket/manifest.json
```

DEBUG mode:
```
DEBUG: Creating local writer for file.parquet
Writing chunk 0 of file 0
...
Written local file file.parquet
DEBUG: NOT uploading to GCS in DEBUG mode
DEBUG: Manifest written locally, skipping GCS upload
```

---

## Modified Functions Summary

### New Functions:
1. **`create_parquet_writer()`** - Creates writer that streams to GCS or writes locally
2. **`write_manifest_to_gcs()`** - Writes manifest directly to GCS

### Modified Functions:
1. **`upload_to_gcs()`** - Updated docstring (now used only for manifest in non-DEBUG mode)
2. **`fetch_and_write()`** - Refactored to use streaming writers
3. **`write_manifest()`** - Updated docstring (now only used in DEBUG mode)

### Updated Documentation:
1. **`CLAUDE.md`** - Fixed S3 → GCS reference
2. **`README.md`** - Added authentication section, updated all S3 references

---

## Files Modified in This Session

1. `/home/manuel/Projects/verifier-alliance/parquet-export/CLAUDE.md`
2. `/home/manuel/Projects/verifier-alliance/parquet-export/README.md`
3. `/home/manuel/Projects/verifier-alliance/parquet-export/main.py`

---

## Next Steps / Recommendations

1. **Test in DEBUG mode locally:**
   ```bash
   DEBUG=1 DEBUG_TABLE=verified_contracts python main.py
   ```

2. **Test streaming in production-like environment:**
   ```bash
   # With GCS credentials
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   DEBUG_TABLE=verified_contracts python main.py
   ```

3. **Monitor first production run:**
   - Watch for memory usage (should be stable)
   - Verify files appear in GCS with correct sizes
   - Check manifest.json is properly generated
   - Confirm no local files are created

4. **Consider future optimizations:**
   - Connection pooling for GCS client (currently creates new client per file)
   - Retry logic for network failures during streaming
   - Progress indicators for long-running exports

5. **Update deployment documentation** if needed:
   - Ensure Cloud Run job has sufficient memory
   - Verify Workload Identity has storage.objects.create permission
   - Document any network requirements for streaming

---

## Technical Notes

### Why PyArrow Works with GCS Blobs

PyArrow's `ParquetWriter` accepts any file-like object that implements:
- `write(bytes)` method
- `close()` method

GCS blob's `open('wb')` returns a `BlobWriter` object that implements these methods, making it compatible with PyArrow's streaming writes.

### Memory Considerations

- Streaming doesn't eliminate all memory usage
- PyArrow still buffers data internally before writing
- Chunk-based processing (already implemented) keeps memory usage bounded
- Overall memory profile should be similar to before (no large increase)

### Error Handling

If network fails during streaming:
- PyArrow will raise an exception
- Partial file may be uploaded to GCS
- Transaction is not atomic
- Consider implementing retry logic or cleanup of partial files

### DEBUG Mode Design

Kept original local file behavior in DEBUG mode because:
1. Easy to inspect generated Parquet files
2. Familiar workflow for developers
3. No GCS credentials needed for local testing
4. Faster iteration during development

---

## Conclusion

Successfully implemented zero-disk streaming to GCS while maintaining backward compatibility with DEBUG mode. The implementation is clean, well-documented, and ready for production testing.
