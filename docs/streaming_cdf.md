# Streaming (delta) Transforms

Streaming BYOC transforms process a Data Lake Object's Change Data Feed continuously instead of reading a bounded snapshot.
This is meant for data engineers who already work with streaming and change data, and who are comfortable with
* Structured Streaming basics: microbatches, stateless versus stateful operations, and why some DataFrame operations (window functions, sorting) aren't allowed on a stream.
* Change feeds: preimages and postimages, deletes, and several changes to one key in a short span.
* Data drift: source schemas and data can change while a stream runs, and your transform has to tolerate that.
* Merge semantics: how a MERGE treats an upsert and a delete, and why two rows for one key in a batch is an error.

## What you Read
Streaming exposes additional columns per row of changed data to give you fine-grained control over the handling of the changes.
These columns are described below
| Column Name | Values | Notes |
| :--- | :--- | :--- |
| _change_type | insert, update_preimage, update_postimage, delete | |
| _commit_version | long | per-source DLO |
| _commit_timestamp | timestamp | when the commit landed in the source DLO, not exact |
| _source_table_name | string | the source DLO for the row |
| _source_record_key | string, opaque | The source DLO's PK value for this row |

## What you Write
The DataFrame you write must have:

* _merge_record_type: UPSERT or DELETE.
* _commit_version, _source_table_name and _source_record_key, as read. The sink uses them to find the latest version of each source record.
* The target DLO's PK columns.

The stream fails at start if any of these is missing. Columns that aren't in the target DLO, including every system column, aren't written.

### Beware of Filtering Mutable Columns
Remember that streaming transforms operate on change data feeds. If you filter the rows based on a column that changed before deciding the _merge_record_type,
you may lose deletes and have stale data in your output. A safe rule of thumb is to filter only on immutable columns. For mutable columns, treat update_preimage
that matches a filter as a delete and update_postimage that matches as an upsert.

```python
def to_merge_records(changes: DataFrame) -> DataFrame:
    # Deletes and preimages become DELETE, everything else UPSERT
    removed = F.col(cdf.CHANGE_TYPE).isin(cdf.ChangeType.DELETE, cdf.ChangeType.UPDATE_PREIMAGE)
    return changes.withColumn(
        cdf.MERGE_RECORD_TYPE,
        F.when(removed, F.lit(cdf.MergeRecordType.DELETE)).otherwise(F.lit(cdf.MergeRecordType.UPSERT)),
    ).drop(cdf.CHANGE_TYPE)

```
### Deduplication of Row Per-Microbatch
The written dataframe cannot contain two rows with the same primary key. The SDK includes a default two-step deduplication pass that first picks the row with
the latest commit version and then picks upserts over deletes. Depending on the shape of your data, this may not be enough to pick a single row. Full control
of the deduplication is available by setting the dedup_order parameters in write_*_deltas methods. The parameter is an ordered list of entries `{"column", "direction", "partition"}`. Each entry narrows, within its partition, to the rows tied on its column. `partition` is optional and names any column in your DataFrame; an entry without it runs per target primary key. Entries with `partition` must come before entries without it. If more than one row is left for a key after the last entry, the batch fails.

`SOURCE_RECORD_STANDARD` provides two dedup_order entries partitioned by `_source_record_key`: keep the latest by `_commit_version`, then prefer UPSERT over DELETE. Add your own entries after it to break ties across source records.

```python
dedup_order=[
    *SOURCE_RECORD_STANDARD,
    {"column": "lead_score__c", "direction": "desc"},
]
```

Pass `dedup_order=[]` to fail the batch whenever two different rows share a key.

## Example
Use a `StreamingClient` and its `*_deltas` methods in place of the batch `Client` read/write methods:

```python
from pyspark.sql.functions import col, upper

from datacustomcode import StreamingClient

client = StreamingClient()

# read_dlo_deltas returns a *streaming* DataFrame over the change feed.
# The runtime resolves the single streaming source, so no name is passed.
deltas = client.read_dlo_deltas()

# Ordinary PySpark transform.
transformed = deltas.withColumn("description__c", upper(col("description__c")))

# write_dlo_deltas starts a streaming query and returns the StreamingQuery.
# The runtime owns the trigger and checkpoint location; you
# choose only the target table.
query = client.write_dlo_deltas("Output__dll", transformed, dedup_order=SOURCE_RECORD_STANDARD)
query.awaitTermination()
```

A streaming transform can also read and write Data Model Objects with `read_dmo_deltas()` and `write_dmo_deltas()`:

```python
deltas = client.read_dmo_deltas()
transformed = deltas.withColumn("description__c", upper(col("description__c")))
query = client.write_dmo_deltas("Output__dlm", transformed, dedup_order=SOURCE_RECORD_STANDARD)
query.awaitTermination()
```

Notes:

- These methods only run inside the Data Cloud streaming (`DELTA_SYNC`) runtime. Locally they raise `NotImplementedError`, since there is no change feed to stream.
- A complete runnable entry point is provided in [`examples/streaming_deltas/entrypoint.py`](src/datacustomcode/templates/script/examples/streaming_deltas/entrypoint.py).
