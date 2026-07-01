"""Streaming BYOC transform: read a DLO change feed and write the deltas back.

This example is the streaming counterpart to a normal batch entrypoint. Instead
of ``read_dlo`` / ``write_to_dlo`` (which read and write a bounded snapshot), it
uses the streaming delta methods:

* ``client.read_dlo_deltas(name)`` returns a *streaming* DataFrame over the
  Change Data Feed of the source DLO. Each row carries the source columns plus
  change-feed metadata columns (``_record_type``, ``_commit_*``).
* ``client.write_dlo_deltas(name, df, write_mode)`` starts a streaming query
  that writes each micro-batch to the target DLO and returns the
  ``StreamingQuery`` handle. The runtime owns the trigger and checkpoint
  location — the caller only chooses the table and write mode.

The transform in between is ordinary PySpark. Because the source is a change
feed, keep the metadata columns on the DataFrame you hand to
``write_dlo_deltas`` — the sink relies on them to merge changes correctly.

This entrypoint only runs inside the Data Cloud streaming (``DELTA_SYNC``)
runtime; the local ``datacustomcode run`` readers/writers raise
``NotImplementedError`` for the delta methods.
"""

from pyspark.sql.functions import col, upper

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode


def main():
    client = Client()

    # Streaming DataFrame over the source DLO's change feed.
    deltas = client.read_dlo_deltas("Account_std__dll")

    # Ordinary PySpark transform. Note we do NOT drop the change-feed metadata
    # columns (those starting with "_") — the streaming sink needs them to apply
    # inserts, updates, and deletes to the target DLO.
    transformed = deltas.withColumn("description__c", upper(col("description__c")))

    # Start the streaming write. write_dlo_deltas returns the StreamingQuery;
    # the trigger and checkpoint location are provided by the runtime.
    query = client.write_dlo_deltas(
        "Account_std_copy__dll", transformed, WriteMode.APPEND
    )

    # Drive the query's lifecycle. In the streaming runtime this blocks until
    # the job is stopped by the platform.
    query.awaitTermination()


if __name__ == "__main__":
    main()
