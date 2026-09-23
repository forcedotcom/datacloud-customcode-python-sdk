# Example: Score leads with a bundled scikit-learn model

A Code Extension for Batch Transform that scores every lead in a `Lead__dll` DLO against a scikit-learn classifier **bundled directly with the deploy package**.

## What it demonstrates

- **Bundling a trained model as `payload/files/lead_scorer.zip`** (a zip wrapping `lead_scorer.joblib`).
- **Grid-join scoring** — instead of scoring each lead individually, the script enumerates every combination of the model's categorical inputs, scores that small grid with `predict_proba`, and turns the results into a reference DataFrame that is left-joined against the leads.

## Regenerating the bundled model

```sh
pip install -r requirements.txt
python train_model.py
```

## Prerequisites

A `Lead__dll` DLO in the target dataspace with at least:

| Column           | Type   |
|------------------|--------|
| `id__c`          | text   |
| `first_name__c`  | text   |
| `last_name__c`   | text   |
| `industry__c`    | text   |
| `employee_band__c` | text |
| `region__c`      | text   |
| `source__c`      | text   |

Data should be loaded via `sample_data/lead.csv`.  See [loading sample data](../README.md#loading-sample-data).

Output DLO `Lead_Scored__dll` must also exist (same columns plus `score__c` number).
