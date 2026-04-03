# Changelog

## 1.0.1

### Added

- **`llm_gateway_generate_text()` UDF wrapper for AI-powered DataFrame transformations.**

  New method on proxy providers to generate AI completions in DataFrame operations via the `llm_gateway_generate` UDF.

  ```python
  from datacustomcode import Client
  from pyspark.sql.functions import col

  client = Client()

  # Generate summaries in a DataFrame column
  df = df.withColumn(
      "summary",
      client._proxy.llm_gateway_generate_text(
          "Summarize {company}: revenue={revenue}, CEO={ceo}",
          {
              "company": col("company"),
              "revenue": col("revenue"),
              "ceo": col("ceo")
          },
          llmModelId="sfdc_ai__DefaultGPT4Omni",
          maxTokens=200
      )
  )
  ```

  **Local Development:** Returns placeholder string (doesn't execute)  
  **BYOC Production:** Calls real `llm_gateway_generate` UDF

  **Parameters:**
  - `template` (str): Prompt template with {placeholder} syntax
  - `values` (dict or Column): Dict mapping placeholders to Columns, or pre-built named_struct
  - `llmModelId` (str): Model identifier (required, e.g., "sfdc_ai__DefaultGPT4Omni")
  - `maxTokens` (int): Maximum response length (required, e.g., 200)


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
