# Example: Iterative rollup of an account hierarchy

A Code Extension for Batch Transform that walks a self-referencing `Account__dll` and produces `Account_Rollup__dll` with each account's **total-tree ARR** — its own ARR plus every descendant account's ARR.

## What it demonstrates

- Imperative control flow (a `while` loop with `isEmpty()` convergence) around DataFrame ops.
- `.persist()` on the frontier + growing set, so each iteration doesn't re-derive earlier hops.
- Alias-based joins (`accounts.alias("a")` / `descendants.alias("d")`) — required whenever the same DataFrame appears on both sides.

## Prerequisites

An `Account__dll` DLO in the target dataspace with columns:

| Column          | Type    | Notes                                       |
|-----------------|---------|---------------------------------------------|
| `id__c`         | text    | Primary key.                                |
| `parent_id__c`  | text    | Nullable. FK back to `Account__dll.id__c`.  |
| `arr__c`        | number  | Annual recurring revenue for this account.  |

Data should be loaded via `sample_data/account.csv`.  See [loading sample data](../README.md#loading-sample-data).

Output DLO `Account_Rollup__dll` must also exist with `id__c` and `tree_arr__c` columns.
