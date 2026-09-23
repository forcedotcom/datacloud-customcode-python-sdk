# Example: Flatten a batched event stream from a JSON-array column

A Code Extension for Batch Transform that reads a `User_Sessions__dll` DLO — one row per session, with each session's events packed into a JSON-array column — and writes one row per event into `User_Events__dll`. Parse-and-explode logic lives in a reusable module under `payload/py-files/`.

## What it demonstrates

- **`payload/py-files/`** — Split real logic into modules, unit-test them locally, reuse them across scripts.
- **`from_json` + `explode_outer`** to turn one row per session into one row per event, carrying the parent `session_id__c` and `user_id__c` through.
- **`to_timestamp()`** to parse ISO-8601 event timestamps into Spark `TimestampType`, which writes cleanly into Data 360's `DateTime` column.

## Prerequisites

Input DLO `User_Sessions__dll` in the target dataspace:

| Column           | Type | Notes                                       |
|------------------|------|---------------------------------------------|
| `session_id__c`  | text | Primary key.                                |
| `user_id__c`     | text |                                             |
| `events__c`      | text | JSON array of `{event_id, event_type, ts, path, value}`. |

Data should be loaded via `sample_data/user_sessions.csv`.  See [loading sample data](../README.md#loading-sample-data).

Output DLO `User_Events__dll` in the same dataspace:

| Column          | Type     | Notes         |
|-----------------|----------|---------------|
| `event_id__c`   | text     | Primary key.  |
| `session_id__c` | text     |               |
| `user_id__c`    | text     |               |
| `event_type__c` | text     |               |
| `event_ts__c`   | DateTime |               |
| `path__c`       | text     |               |
| `value__c`      | text     |               |
