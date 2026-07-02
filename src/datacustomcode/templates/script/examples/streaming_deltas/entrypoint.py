"""Streaming BYOC transform: read a DLO change feed and write the deltas back.

This example is the streaming counterpart to a normal batch entrypoint. Instead
of ``read_dlo`` / ``write_to_dlo`` (which read and write a bounded snapshot), it
uses the streaming delta methods:

* ``client.read_dlo_deltas(name)`` returns a *streaming* DataFrame over the
  Change Data Feed of the source DLO. Each row carries the source columns plus
  change-feed metadata columns (``_record_type``, ``_commit_*``).
* ``client.write_dlo_deltas(name, df)`` starts a streaming query that writes
  each micro-batch to the target DLO and returns the ``StreamingQuery`` handle.
  The runtime owns the trigger, and checkpoint location — the caller only
  chooses the table.

The transform in between is ordinary PySpark. Because the source is a change
feed, keep the metadata columns on the DataFrame you hand to
``write_dlo_deltas`` — the sink relies on them to merge changes correctly.

The local ``datacustomcode run`` readers/writers raise
``NotImplementedError`` for the delta methods.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

from datacustomcode.client import (
    Client,
    RunMode,
    get_run_mode,
)


def main():
    client = Client()
    source_dlo = "Account_std__dll"
    target_dlo = "Account_std_copy__dll"
    if get_run_mode() == RunMode.DELTA_SYNC:
        # Streaming DataFrame over the source DLO's change feed.
        dataframe = client.read_dlo_deltas(source_dlo)
        # Ordinary PySpark transform.
        transformed = transform(dataframe)

        # Start the streaming write. write_dlo_deltas returns the StreamingQuery;
        # the trigger and checkpoint location are provided by the runtime.
        query = client.write_dlo_deltas(target_dlo, transformed)

        # Drive the query's lifecycle. In the streaming runtime this blocks until
        # the job is stopped by the platform.
        query.awaitTermination()
    else:
        dataframe = client.read_dlo(source_dlo)
        transformed = transform(dataframe)
        client.auto_write_to_dlo(target_dlo, transformed)


def transform(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn("description__c", upper(col("description__c")))


if __name__ == "__main__":
    main()
