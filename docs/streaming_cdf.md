### Streaming (delta) transforms

Streaming BYOC transforms process a Data Lake Object's Change Data Feed continuously instead of reading a bounded snapshot. Use a `StreamingClient` and its `*_deltas` methods in place of the batch `Client` read/write methods:

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
query = client.write_dlo_deltas("Output__dll", transformed)
query.awaitTermination()
```

A streaming transform can also read and write Data Model Objects with `read_dmo_deltas()` and `write_dmo_deltas()`:

```python
deltas = client.read_dmo_deltas()
transformed = deltas.withColumn("description__c", upper(col("description__c")))
query = client.write_dmo_deltas("Output__dlm", transformed)
query.awaitTermination()
```

Notes:

- These methods only run inside the Data Cloud streaming (`DELTA_SYNC`) runtime. Locally they raise `NotImplementedError`, since there is no change feed to stream.
- A complete runnable entry point is provided in [`examples/streaming_deltas/entrypoint.py`](src/datacustomcode/templates/script/examples/streaming_deltas/entrypoint.py).
