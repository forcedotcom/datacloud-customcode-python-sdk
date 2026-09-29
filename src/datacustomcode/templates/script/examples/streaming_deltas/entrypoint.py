"""Streaming BYOC transform: read a DLO change feed and write the deltas back.

This example is the streaming counterpart to a normal batch entrypoint. Instead
of a batch ``Client`` with ``read_dlo`` / ``write_to_dlo`` (which read and write
a bounded snapshot), it uses a :class:`StreamingClient` and its streaming delta
methods.

The first run of a streaming job will use the run mode INITIAL_SYNC which behaves
like a batch run on the streaming source. A streaming transform can also use run
mode REBUILD to do the same thing on demand. Note that these will process all
source rows and overwrite the target.

The transform in between is ordinary PySpark. Because the source is a change
feed, keep the metadata columns on the DataFrame you hand to
``write_dlo_deltas`` — the sink relies on them to merge changes correctly.

This entrypoint only runs inside the Data Cloud runtime;
 the local ``datacustomcode run`` readers/writers raise
``NotImplementedError`` for the delta methods.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

from datacustomcode.client import (
    RunMode,
    StreamingClient,
    get_run_mode,
)


def main():
    target_dlo = "Account_std_copy__dll"
    client = StreamingClient()

    if get_run_mode() == RunMode.DELTA_SYNC:
        # Streaming DataFrame over the source DLO's change feed.
        dataframe = client.read_dlo_deltas()
        # Ordinary PySpark transform.
        transformed = transform(dataframe)

        # Start the streaming write. write_dlo_deltas returns the StreamingQuery;
        # the trigger and checkpoint location are provided by the runtime.
        query = client.write_dlo_deltas(target_dlo, transformed)

        # Drive the query's lifecycle. In the streaming runtime this blocks until
        # the job is stopped by the platform.
        query.awaitTermination()
    else:
        # initial sync and rebuild read the entire streaming source DLO and
        # write using a server-decided mode based on the run mode
        dataframe = client.read_dlo()
        transformed = transform(dataframe)
        client.auto_write_to_dlo(target_dlo, transformed)


def transform(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn("description__c", upper(col("description__c")))


if __name__ == "__main__":
    main()
