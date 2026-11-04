# Chunking with a Gemini Named Credential Callout

Splits each input document into paragraph-sized chunks and calls Google's
**Gemini** `generateContent` API for every chunk. The model returns a summary,
category, sentiment, and topics, which are attached to the chunk as citations so
the search index can filter and rank on them. Gemini is reached through a
**Named Credential**, so this code never handles the endpoint URL or the API key.

## How the callout works

```python
CALLOUT_URL = "callout:gemini"  # callout:<NC name>[/<path>]

request = (
    HTTPRequestBuilder()
    .set_url(CALLOUT_URL)
    .set_method(HTTPMethod.POST)
    .set_headers({"Content-Type": "application/json"})
    .set_response_timeout_seconds(60) # optional: per-callout timeout (seconds)
    .build()
)
# Body is sent verbatim (serialize it yourself); the response body is a raw string.
response = runtime.named_credential.request(request, json.dumps(payload))
if response.is_success:
    envelope = json.loads(response.body)
    text = envelope["candidates"][0]["content"]["parts"][0]["text"]
```

The request asks for `responseMimeType: application/json` with a `responseSchema`,
so Gemini returns the classification as a JSON string in
`candidates[0].content.parts[0].text` — decode it, then decode that text again.

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

```bash
DATACUSTOMCODE_EXTERNAL_CALLOUT_CONFIG=/abs/path/to/external_callout_config.json \
  sf data-code-extension function run \
    --entrypoint payload/entrypoint.py \
    --test-with payload/tests/test.json \
    --target-org <your-org-alias>
```

With `--target-org` the SDK fetches only the **URL** from the org's Named
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

## Auth types

`auth_type` selects how auth is injected for local testing. It should mirror the
External Credential your Named Credential uses in the org, so local and deployed
runs behave the same. This example uses `Custom` (Gemini's `X-goog-api-key`);
all four supported types:

```json
{
    "credentials": {
        "callout:my_custom_api": {
            "auth_type": "Custom",
            "custom_headers": { "X-goog-api-key": "YOUR_API_KEY" }
        },
        "callout:my_basic_api": {
            "auth_type": "Basic",
            "username": "svc_user",
            "password": "YOUR_PASSWORD"
        },
        "callout:my_oauth_api": {
            "auth_type": "OAuth",
            "access_token": "YOUR_ACCESS_TOKEN"
        },
        "callout:my_jwt_api": {
            "auth_type": "Jwt",
            "token": "YOUR_JWT"
        }
    }
}
```

| `auth_type` | Fields read | Header sent |
| ----------- | ------------------------------- | --------------------------------------- |
| `Basic`     | `username`, `password`          | `Authorization: Basic <base64 user:pw>` |
| `Custom`    | `custom_headers` (sent verbatim)| the headers you list                    |
| `OAuth`     | `access_token` or `token`       | `Authorization: Bearer <token>`         |
| `Jwt`       | `access_token` or `token`       | `Authorization: Bearer <token>`         |

`OAuth`/`Jwt` take a token you supply for the local run — the SDK does not fetch
or refresh it. In the Data Cloud runtime the Named Credential handles token
acquisition; this local config only stands in for that during testing.
