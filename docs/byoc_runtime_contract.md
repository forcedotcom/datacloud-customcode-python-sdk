# BYOC Runtime Contract

This document defines the contract between `salesforce-data-customcode` (the SDK)
and the Data Cloud Code Extension runtimes — script packages and function packages — for filesystem-bundled assets
referenced via `Client.find_file_path`.

## LIBRARY_PATH

The runtime MUST set `LIBRARY_PATH` to the directory that *contains* the
extracted package's `files/` directory — i.e., the package root, the same
directory that holds `config.json` and `entrypoint.py`.

Concretely:

```
$LIBRARY_PATH/
├── config.json
├── entrypoint.py
└── files/
    └── <bundled assets>
```

Given this layout, `client.find_file_path("data.csv")` resolves to
`$LIBRARY_PATH/files/data.csv`. Relative subpaths under `files/` are supported:
`client.find_file_path("dir/data.csv")` resolves to
`$LIBRARY_PATH/files/dir/data.csv`.

## WORKDIR

The customer-function `WORKDIR` MAY be `/app` (or anything else). The SDK does
not depend on `cwd` for file resolution when `LIBRARY_PATH` is set. Calling
`os.chdir($LIBRARY_PATH)` from runtime code is no longer required and should be
removed when the runtime is updated to a `salesforce-data-customcode` release
that includes this contract.

## File layout

Customer packages place bundled files under `payload/files/<name>`. After
extraction in the runtime, the file lives at `$LIBRARY_PATH/files/<name>`.
Files placed directly at the package root (`payload/<name>`, extracted to
`$LIBRARY_PATH/<name>`) are still resolvable but the canonical location is
`files/`.

## Resolution order

`Client.find_file_path` tries candidate paths in this order, returning the
first one that exists:

1. `$BASE_PATH/files/<name>`, then `$BASE_PATH/<name>` — when the SDK is
   constructed with an explicit `base_path`.
2. `$LIBRARY_PATH/files/<name>`, then `$LIBRARY_PATH/<name>` — when
   `LIBRARY_PATH` is set.
3. `payload/files/<name>` relative to cwd — the local `datacustomcode run`
   flow from a project root.
4. `<config_dir>/files/<name>` discovered by walking the cwd subtree for
   `config.json`.

If no candidate exists, `FileNotFoundError` is raised; the message lists every
path that was tried.

## Versioning

This contract applies to `salesforce-data-customcode` releases that include
the `$LIBRARY_PATH/files/<name>` lookup step. Older runtime images pinned to
prior SDK releases must continue to set `cwd = $LIBRARY_PATH` (script
runtime's existing behavior) or `os.chdir($LIBRARY_PATH)` (function runtime
workaround) until they upgrade.
