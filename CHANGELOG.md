# Changelog

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
