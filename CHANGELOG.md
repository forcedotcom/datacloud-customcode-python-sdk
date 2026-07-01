# Changelog

## 6.1.0

### Added

- **Streaming (delta) read/write methods on `Client` for BYOC streaming transforms.**

  New methods let an entry point process a Data Lake Object's Change Data Feed continuously instead of reading a bounded snapshot:

  - `read_dlo_deltas(name)` / `read_dmo_deltas(name)` – return a streaming DataFrame over the object's change feed.
  - `write_dlo_deltas(name, dataframe, write_mode)` – start a streaming query that writes each micro-batch to the target DLO and return the `StreamingQuery` handle.

  ```python
  deltas = client.read_dlo_deltas("Input__dll")
  transformed = deltas.withColumn("description__c", upper(col("description__c")))
  query = client.write_dlo_deltas("Output__dll", transformed, WriteMode.APPEND)
  query.awaitTermination()
  ```

  Supported streaming write modes are `WriteMode.APPEND`, `WriteMode.OVERWRITE`, and `WriteMode.MERGE_UPSERT_DELETE`. These methods run only inside the Data Cloud streaming (`DELTA_SYNC`) runtime; locally they raise `NotImplementedError`. See the `examples/streaming_deltas/entrypoint.py` example and the "Streaming (delta) transforms" section of the README.

## 6.0.0

### Breaking Changes

- **`WriteMode.OVERWRITE_PARTITIONS` is deprecated and now raises an error if used.**

  `WriteMode.OVERWRITE_PARTITIONS` remains in the enum for backward compatibility but will raise a `ValueError` at runtime.

  **Migration:** Use `WriteMode.OVERWRITE` to replace all records, or `WriteMode.MERGE` for targeted updates by primary key:

  ```python
  # Before
  client.write_to_dlo("MyTable__dll", df, write_mode=WriteMode.OVERWRITE_PARTITIONS)

  # After (full replacement)
  client.write_to_dlo("MyTable__dll", df, write_mode=WriteMode.OVERWRITE)

  # After (update specific rows by primary key)
  client.write_to_dlo("MyTable__dll", df, write_mode=WriteMode.MERGE)
  ```

## 3.0.0

### Breaking Changes

- **Added  `runtime: datacustomcode.runtime.function.Runtime` to function contract for codeType `function`.

  Function now mandates runtime as arguments.

  **Why:** `runTime` allows access to resources ( llm_gateway / file ) available during function execution.

  **Migration:** use function(request: dict, runTime: Runtime) instead od function(request: dict)

  ```python
  # Before
  def function(request: dict):
    pass

  # After
  def function(request: dict, runTime: Runtime):
    pass
  ```


## 2.0.0

### Breaking Changes

- **Removed the `row_limit` parameter from `read_dlo()` and `read_dmo()`.**

  These methods no longer accept a `row_limit` argument. When running locally, reads are automatically capped at 1000 rows to prevent accidentally fetching large datasets during development. When deployed to Data Cloud, no limit is applied and all records are returned.

  **Why:** The `row_limit` parameter duplicated PySpark's built-in `.limit()` and created a behavioral difference between local and deployed environments. The 1000-row safety net is now handled internally via the `default_row_limit` setting in `config.yaml`, and deployed environments naturally omit it.

  **Migration:** Remove any `row_limit` arguments from your `read_dlo()` and `read_dmo()` calls. If you need a specific number of rows, use PySpark's `.limit()` on the returned DataFrame:

  ```python
  # Before
  df = client.read_dlo("MyObject__dll", row_limit=500)

  # After
  df = client.read_dlo("MyObject__dll").limit(500)
  ```

## 1.0.0

### Breaking Changes

- **`read_dlo()` and `read_dmo()` now return DataFrames with all-lowercase column names.**

  Column names returned by both `QueryAPIDataCloudReader` and `SFCLIDataCloudReader` are now lowercased to match the column names produced by the deployed Data Cloud environment (e.g., `unitprice__c` instead of `UnitPrice__c`).

  **Why:** In the deployed environment, column names are normalized to lowercase by the underlying Iceberg metadata layer. The local SDK previously returned the original API casing, causing "column does not exist" errors when scripts were deployed. This change aligns local behavior with the cloud.

  **Migration:** Update any column references in your local scripts to use lowercase:

  ```python
  # Before
  df.withColumn("Description__c", upper(col("Description__c")))
  df.drop("KQ_Id__c")
  df["UnitPrice__c"]

  # After
  df.withColumn("description__c", upper(col("description__c")))
  df.drop("kq_id__c")
  df["unitprice__c"]
  ```

  Scripts already running in Data Cloud are unaffected — the cloud always returned lowercase column names.
