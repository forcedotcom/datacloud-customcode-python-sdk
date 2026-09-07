# Transform with a Gemini Named Credential Callout

The **transform** calls Google's **Gemini** `generateContent` API to summarize text, and
write the result back to a DLO. Gemini is reached through a **Named Credential**
(`callout:gemini`), so this code never handles the endpoint URL or the API key.

It shows **both** callout paths against the same Named Credential.

## Shared request template

```python
from datacustomcode.client import Client, named_credential_request_col

# URL, method and headers apply to every callout on this template.
request = (
    HTTPRequestBuilder()
    .set_url("callout:gemini")            # callout:<NC name>[/<path>]
    .set_method(HTTPMethod.POST)
    .set_headers({"Content-Type": "application/json"})
    .set_response_timeout_seconds(60)     # optional: per-callout timeout (seconds)
    .build()
)
```

## Driver path — one-shot on the driver

`Client.named_credential_request` runs the callout **once** on the driver and
returns an `HTTPResponse` (`.status_code`, `.body`, `.headers`, `.is_success`).
Use it for a lookup or a shared value you reuse across the job.

```python
response = client.named_credential_request(request, body=json.dumps(payload))
if response.is_success:
    envelope = json.loads(response.body)
    text = envelope["candidates"][0]["content"]["parts"][0]["text"]
```

## Per-row path — fan out across the DataFrame

`named_credential_request_col` dispatches one callout per row; only the body
Column varies.

```python
# One callout per row; body is a Column built from the row's data.
df = df.withColumn("_callout", named_credential_request_col(request, body=body_col))
```

It returns a struct Column:

```
{status, response: {status_code, body, headers}, error_code, error_message}
```

- A non-2xx response is still `status = "SUCCESS"` with the HTTP code in
  `response.status_code` — extracting the model text just yields null for that row.
- A transport failure sets `status = "ERROR"`; the row survives, the job does not
  abort. Pull the model text out of `response.body` with `get_json_object(...)`.

Both paths resolve the same Named Credential and read auth from the same local
`external_callout_config.json`.

The `gemini` Named Credential's URL already includes the full
`/v1beta/models/<model>:generateContent` path, so the callout is just
`callout:gemini` with **no path suffix** (anything after the name is appended to
the credential's URL).

## Configure the Named Credential

1. Create an **External Credential** (e.g. `google_api_key`) that injects your
   Gemini API key as the `X-goog-api-key` header.
2. Create a **Named Credential** named `gemini`:
   - **URL**: `https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent`
   - **Enabled for Callouts** + **Generate Authorization Header**: on
   - **External Credential**: `google_api_key`

## Test locally

Copy `entrypoint.py` into your `payload/` folder (or point the run at it), then:

```bash
DATACUSTOMCODE_EXTERNAL_CALLOUT_CONFIG=/abs/path/to/external_callout_config.json \
  sf data-code-extension script run --entrypoint entrypoint.py --target-org <your-org-alias>
```

With `----target-org` the SDK fetches only the **URL** from the org's Named
Credential; **auth is always taken from `external_callout_config.json`** locally
(the org's External Credential is used only in the Data Cloud runtime). So the
`X-goog-api-key` must be in the local config for a local test. Omit
`--target-org` to run fully offline using `target_url`.

```json
{
    "credentials": {
        "callout:gemini": {
            "auth_type": "Custom",
            "custom_headers": { "X-goog-api-key": "YOUR_GEMINI_API_KEY" },
            "target_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent"
        }
    }
}
```

Place `external_callout_config.json` in the **parent of your payload folder** (or
point `DATACUSTOMCODE_EXTERNAL_CALLOUT_CONFIG` at it). It is never packaged into
the deployment zip. Get a key from [Google AI Studio](https://aistudio.google.com/apikey);
**do not commit it.**

## What it reads / writes

`config.json` declares the DLO permissions for deployment:

| | DLO | Notes |
| ------ | ------------------- | --------------------------------------------- |
| read   | `Account_std__dll`  | source rows; `description__c` is summarized   |
| write  | `Account_std_copy__dll` | adds `summary__c`, `callout_status__c`, `callout_http_code__c` |

Adjust `_TEXT_COLUMN`, `_SOURCE_DLO` and `_TARGET_DLO` in `entrypoint.py` (and the
matching entries in `config.json`) to point at your own DLOs.

## Auth types

`auth_type` selects how auth is injected for local testing. It should mirror the
External Credential your Named Credential uses in the org, so local and deployed
runs behave the same. This example uses `Custom` (Gemini's `X-goog-api-key`);
all four supported types:

| `auth_type` | Fields read | Header sent |
| ----------- | ------------------------------- | --------------------------------------- |
| `Basic`     | `username`, `password`          | `Authorization: Basic <base64 user:pw>` |
| `Custom`    | `custom_headers` (sent verbatim)| the headers you list                    |
| `OAuth`     | `access_token` or `token`       | `Authorization: Bearer <token>`         |
| `Jwt`       | `access_token` or `token`       | `Authorization: Bearer <token>`         |

`OAuth`/`Jwt` take a token you supply for the local run — the SDK does not fetch
or refresh it. In the Data Cloud runtime the Named Credential handles token
acquisition; this local config only stands in for that during testing.
