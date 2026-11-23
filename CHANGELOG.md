# Changelog

## 7.1.0

### Added

- **Per-row `path` in `named_credential_request_col`.**

  `named_credential_request_col(request, body=None, path=None)` (and `SparkNamedCredential.request_col`) accepts an optional per-row `path` Column that is appended to `request.url`, so each row can call a different endpoint of the same Named Credential (for example, per-entity REST lookups). A path not starting with `/` or `?` is joined with `/`. A null or empty value uses `request.url` as is. The Named Credential itself cannot be changed per row.

  ```python
  from pyspark.sql.functions import col, concat, lit
  from datacustomcode.client import named_credential_request_col

  df = df.withColumn(
      "result",
      named_credential_request_col(request, path=concat(lit("?id="), col("id__c"))),
  )
  ```

  Omitting `path` keeps the existing behavior.

### Security

- **PKCE and `state` validation for the browser OAuth login.** The authorization-code flow now sends a PKCE (RFC 7636, S256) code challenge and checks the `state` parameter on the local callback. Callbacks with a missing or mismatched `state` are rejected.
- **Secrets are redacted from debug logs.** `Authorization`, `Proxy-Authorization`, `Cookie`, and `Set-Cookie` header values are logged as `[REDACTED]`, and request keyword arguments are logged by name only.

### Changed

- **Clearer function signature-mismatch error on `deploy`.** The error now lists the supported Request/Response pairs instead of naming only the search-index chunking types.
- **`init` for functions uses the template's sample `tests/test.json`** when the template ships one. It falls back to generating a test file from the request model only if the template has none.

## 7.0.0

### Breaking Changes

- **`datacustomcode zip` and `datacustomcode deploy` no longer package symlinks.**

  Symlinks found inside the payload directory are skipped instead of being followed. A symlinked file logs a `Skipping symlink file <path>` warning and is left out of `deployment.zip`.

  **Why:** Following a symlink packaged the bytes of its target into the archive, even when the target was outside the payload (for example `payload/notes.txt -> ~/.ssh/id_rsa`).

  **Migration:** Replace any symlink in the payload with a real copy of the file or directory it points to:

  ```bash
  # Before
  ln -s ../shared/helpers.py payload/helpers.py

  # After
  cp ../shared/helpers.py payload/helpers.py
  ```

### Added

- **`StreamingClient.write_dmo_deltas(name, dataframe)`** – start a streaming query that writes each micro-batch to the target DMO and return the `StreamingQuery` handle. It is the DMO counterpart to `write_dlo_deltas` and pairs with `read_dmo_deltas()`.

## 6.1.0

### Added

- **`StreamingClient` for BYOC streaming (delta) transforms.**

  A dedicated `StreamingClient` (alongside the batch `Client`) lets an entry point process a Data Lake Object's Change Data Feed continuously instead of reading a bounded snapshot.

  - `read_dlo_deltas()` / `read_dmo_deltas()` – return a streaming DataFrame over the object's change feed.
  - `write_dlo_deltas(name, dataframe)` – start a streaming query that writes each micro-batch to the target DLO and return the `StreamingQuery` handle.

  The shared functions (`find_file_path`, `llm_gateway_generate_text`, `einstein_predict`) are available on both `Client` and `StreamingClient`.

  ```python
  from datacustomcode import StreamingClient

  client = StreamingClient()
  deltas = client.read_dlo_deltas()
  transformed = deltas.withColumn("description__c", upper(col("description__c")))
  query = client.write_dlo_deltas("Output__dll", transformed)
  query.awaitTermination()
  ```

  These methods run only inside the Data Cloud streaming (`DELTA_SYNC`) runtime; locally they raise `NotImplementedError`. See the `examples/streaming_deltas/entrypoint.py` example and the "Streaming (delta) transforms" section of the README.

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
