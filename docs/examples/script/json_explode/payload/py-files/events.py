from pyspark.sql.functions import (
    col,
    explode_outer,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("path", StringType(), True),
        StructField("value", StringType(), True),
    ]
)
EVENT_ARRAY_SCHEMA = ArrayType(EVENT_SCHEMA, True)


def parse_events(df, session_col, user_col, events_col):
    """One row per event, carrying its parent session + user.

    Handles both a real ArrayType<Struct> column and a string column that
    contains the JSON array.
    """
    field = next(f for f in df.schema.fields if f.name == events_col)
    events = (
        col(events_col)
        if isinstance(field.dataType, ArrayType)
        else from_json(col(events_col).cast("string"), EVENT_ARRAY_SCHEMA)
    )
    return df.select(
        col(session_col),
        col(user_col),
        explode_outer(events).alias("evt"),
    ).select(
        col("evt.event_id").alias("event_id__c"),
        col(session_col),
        col(user_col),
        col("evt.event_type").alias("event_type__c"),
        to_timestamp(col("evt.ts")).alias("event_ts__c"),
        col("evt.path").alias("path__c"),
        col("evt.value").alias("value__c"),
    )
