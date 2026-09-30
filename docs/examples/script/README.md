# Code Extension for Batch Transform — Examples

End-to-end Code Extension scripts. Each subdirectory is a self-contained deploy package (`payload/entrypoint.py` + `config.json` + `requirements.txt` + any bundled assets) that can run against a Data 360 org.

## Loading sample data

Many examples ship with a `sample_data/` folder containing CSV(s) needed to exercise it. To load one:

1. In Data 360, go to **Data Streams → New** and pick the **File Upload** connector.
2. Upload the CSV from `<example-name>/sample_data/`.
3. Map columns to the target object listed in the example's README, in your dataspace.
4. Run the ingest.

## Related

- Developer guide — [Data 360 Code Extension](https://developer.salesforce.com/docs/data/data-cloud-code-ext/guide/use-custom-code.html).
