# Example: Totals that keep their currency code

A Code Extension for Batch Transform that sums `Payment__dll` amounts per customer and writes them to `Customer_Payment_Totals__dll`, a DLO with a Currency field, setting the `cdp_sys_record_currency__c` system field so Data Explorer UI can display the totals.

## What it demonstrates

- **Carrying `cdp_sys_record_currency__c` through** from the source rows to the output. Code Extensions don't auto-populate `cdp_sys_record_currency__c`. And if it's null, Data Explorer shows the Currency value as blank. See [Writing Currency fields](../../../../README.md#writing-currency-fields).
- **Grouping by currency** so EUR and USD amounts are never summed together. Customer `C1` gets one EUR row and one USD row.
- If all amounts are in one known currency, one can also use `lit("EUR")` instead of copying a column.

## Prerequisites

Input DLO `Payment__dll` in the target dataspace:

| Column                       | Type     | Notes                                                                  |
|------------------------------|----------|------------------------------------------------------------------------|
| `id__c`                      | text     | Primary key.                                                           |
| `customer_id__c`             | text     |                                                                        |
| `amount__c`                  | Currency |                                                                        |
| `cdp_sys_record_currency__c` | text     | System field, added with the Currency field. From CSV `currency_code`. |

Data should be loaded via `sample_data/payment.csv`.  See [loading sample data](../README.md#loading-sample-data).

Output DLO `Customer_Payment_Totals__dll` in the same dataspace:

| Column                       | Type     | Notes                                            |
|------------------------------|----------|--------------------------------------------------|
| `id__c`                      | text     | Primary key (`<customer_id>_<currency>`).        |
| `customer_id__c`             | text     |                                                  |
| `total_amount__c`            | Currency |                                                  |
| `cdp_sys_record_currency__c` | text     | System field, added with the Currency field.     |

The ISO codes in `sample_data/payment.csv` must exist in your org's currency rates.
