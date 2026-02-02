# Code Flow Explanation: SF CLI vs Traditional (Connected App) Authentication

This document explains the code flow from command execution to API call, showing how the system decides whether to use SF CLI direct authentication or traditional OAuth authentication.

## Code Flow: `datacustomcode run ./payload/entrypoint.py --sf-org myorg`

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. CLI ENTRY POINT                                              │
│    File: src/datacustomcode/cli.py                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    @cli.command()
    def run(entrypoint, config_file, dependencies, profile, sf_org):
        ↓
        if sf_org and profile != "default":
            ❌ Error: Cannot use both
        ↓
        run_entrypoint(entrypoint, config_file, dependencies,
                      profile, sf_org)  ← Passes sf_org down

┌─────────────────────────────────────────────────────────────────┐
│ 2. RUN ENTRYPOINT                                               │
│    File: src/datacustomcode/run.py                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    def run_entrypoint(..., sf_org):
        ↓
        # Configure reader and writer
        if sf_org:
            _set_config_option(config.reader_config,
                             "org_alias", sf_org)  ← Key decision!
            _set_config_option(config.writer_config,
                             "org_alias", sf_org)
        elif profile != "default":
            _set_config_option(config.reader_config,
                             "credentials_profile", profile)
        ↓
        runpy.run_path(entrypoint)  ← Execute user's code

┌─────────────────────────────────────────────────────────────────┐
│ 3. USER'S CODE EXECUTION                                        │
│    File: ./payload/entrypoint.py (user's script)               │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    from datacustomcode import Client

    client = Client()  ← Creates client
    df = client.read_dlo("Account__dll")  ← Calls read

┌─────────────────────────────────────────────────────────────────┐
│ 4. CLIENT INITIALIZATION                                        │
│    File: src/datacustomcode/client.py                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    class Client:
        def __new__(cls, reader=None, writer=None, ...):
            ↓
            # Reader initialization
            if reader is None:
                reader_init = config.reader_config.to_object(spark)
                #              ↑
                #              Calls reader's __init__ with options
                #              including org_alias if set
            ↓
            cls._instance._reader = reader_init

┌─────────────────────────────────────────────────────────────────┐
│ 5. READER INITIALIZATION - THE KEY DECISION POINT!              │
│    File: src/datacustomcode/io/reader/query_api.py              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    class QueryAPIDataCloudReader:
        def __init__(self, spark, credentials_profile="default",
                     dataspace=None, org_alias=None):  ← Gets org_alias
            ↓
            ┌──────────────────────────────────────┐
            │ DECISION POINT                       │
            └──────────────────────────────────────┘
            if org_alias:  ← If --sf-org was used
                ┌────────────────────────────┐
                │ USE SF CLI READER          │
                └────────────────────────────┘
                self._sf_cli_reader = SFCLIDataCloudReader(
                    spark=spark,
                    org_alias=org_alias,  ← Passes org alias
                    dataspace=dataspace
                )
                self._conn = None
            else:  ← If --profile was used (or default)
                ┌────────────────────────────┐
                │ USE TRADITIONAL READER     │
                └────────────────────────────┘
                credentials = Credentials.from_available(
                    profile=credentials_profile
                )
                if credentials.auth_type == AuthType.SF_CLI:
                    # Profile configured with SF CLI
                    self._sf_cli_reader = SFCLIDataCloudReader(
                        org_alias=credentials.sf_org_alias
                    )
                else:
                    # Traditional oauth_tokens or client_credentials
                    self._conn = create_cdp_connection(credentials)
                    self._sf_cli_reader = None

┌─────────────────────────────────────────────────────────────────┐
│ 6. READ OPERATION                                               │
│    File: src/datacustomcode/io/reader/query_api.py              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    def read_dlo(self, name, schema=None, row_limit=1000):
        ↓
        if self._sf_cli_reader:  ← Route decision
            ┌────────────────────────────┐
            │ USE SF CLI READER          │
            └────────────────────────────┘
            return self._sf_cli_reader.read_dlo(
                name, schema, row_limit
            )  ─────────────────────────────┐
        else:                                │
            ┌────────────────────────────┐  │
            │ USE CDP CONNECTOR          │  │
            └────────────────────────────┘  │
            query = f"SELECT * FROM {name}" │
            pandas_df = self._conn.         │
                get_pandas_dataframe(query) │
            return spark.createDataFrame(   │
                pandas_df, schema           │
            )                               │
                                            │
┌───────────────────────────────────────────┘
│ 7A. SF CLI READER (if org_alias was provided)
│     File: src/datacustomcode/io/reader/sf_cli.py
└─────────────────────────────────────────────────────────────────┘
                              ↓
    class SFCLIDataCloudReader:
        def read_dlo(self, name, schema, row_limit):
            ↓
            sql = f"SELECT * FROM {name}"
            pandas_df = self._execute_query(sql, row_limit)
            ↓
        def _execute_query(self, sql, row_limit):
            ↓
            # Fetch token from SF CLI
            access_token, instance_url = self._get_sf_cli_token()
            ↓
        def _get_sf_cli_token(self):
            ↓
            result = subprocess.run([
                "sf", "org", "display",
                "--target-org", self.org_alias,
                "--json"
            ])  ← Calls SF CLI command
            ↓
            org_data = json.loads(result.stdout)
            access_token = org_data["result"]["accessToken"]
            instance_url = org_data["result"]["instanceUrl"]
            ↓
            return access_token, instance_url
            ↓
        # Back to _execute_query
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{instance_url}/services/data/v66.0/ssot/query-sql"
        ↓
        response = requests.post(
            url,
            json={"sql": limited_sql},
            params={"dataspace": self.dataspace},
            headers=headers
        )  ← Direct REST API call!
        ↓
        result = response.json()
        rows = result.get("data", [])
        metadata = result.get("metadata", [])
        column_names = [col.get("name") for col in metadata]
        ↓
        return pd.DataFrame(rows, columns=column_names)

┌─────────────────────────────────────────────────────────────────┐
│ 7B. CDP CONNECTOR (if profile was used with oauth_tokens)      │
│     File: salesforcecdpconnector library (external)             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
    SalesforceCDPConnection.get_pandas_dataframe(query)
        ↓
        # 1. Get OAuth access token
        POST /services/oauth2/token
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret
        }
        ↓
        # 2. Exchange for CDP token
        POST /services/a360/token
        params = {
            "grant_type": "urn:salesforce:grant-type:external:cdp",
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": access_token
        }
        ↓
        # 3. Use CDP token for query
        POST /services/a360/query
        headers = {"Authorization": f"Bearer {cdp_token}"}
        ↓
        return pandas_dataframe
```

## Summary of Decision Points

### **Decision Point 1: cli.py (Command Line)**
```python
if sf_org:
    # User specified --sf-org
    pass sf_org to run_entrypoint()
else:
    # User specified --profile or using default
    pass profile to run_entrypoint()
```

### **Decision Point 2: run.py (Configuration)**
```python
if sf_org:
    config.reader_config.options["org_alias"] = sf_org  ← Sets org_alias
elif profile != "default":
    config.reader_config.options["credentials_profile"] = profile
```

### **Decision Point 3: query_api.py (Reader Initialization)**
```python
def __init__(self, ..., org_alias=None):
    if org_alias:
        self._sf_cli_reader = SFCLIDataCloudReader(...)  ← Use SF CLI
    else:
        credentials = Credentials.from_available(...)
        if credentials.auth_type == AuthType.SF_CLI:
            self._sf_cli_reader = SFCLIDataCloudReader(...)  ← Use SF CLI
        else:
            self._conn = SalesforceCDPConnection(...)  ← Use CDP Connector
```

### **Decision Point 4: query_api.py (Read Operation)**
```python
def read_dlo(self, name, ...):
    if self._sf_cli_reader:
        return self._sf_cli_reader.read_dlo(...)  ← Route to sf_cli.py
    else:
        return self._conn.get_pandas_dataframe(...)  ← Route to CDP connector
```

## Quick Reference Table

| User Command | org_alias Set? | Reader Used | Auth Method | API Endpoint |
|--------------|----------------|-------------|-------------|--------------|
| `--sf-org myorg` | ✅ Yes | `SFCLIDataCloudReader` | SF CLI token | `/ssot/query-sql` |
| `--profile myprofile` (sf_cli auth) | ❌ No (uses profile) | `SFCLIDataCloudReader` | SF CLI token | `/ssot/query-sql` |
| `--profile myprofile` (oauth_tokens) | ❌ No | `SalesforceCDPConnection` | OAuth refresh | `/a360/query` |
| Default (no flags) | ❌ No | `SalesforceCDPConnection` | OAuth refresh | `/a360/query` |

The key insight: **`org_alias` parameter is the routing flag** that determines whether to use SF CLI direct API or traditional CDP connector!

## Authentication Mechanism Comparison

### Original Mechanism (OAuth with salesforcecdpconnector)

**Flow:**
```
1. User runs: datacustomcode configure --auth-type oauth_tokens
2. Manually provides:
   - login_url
   - client_id (Connected App)
   - client_secret (Connected App)
   - refresh_token (from OAuth flow)
3. Stores in ~/.datacustomcode/credentials.ini

4. When running code:
   ├─ Read credentials from credentials.ini
   ├─ Use salesforcecdpconnector library
   ├─ Call /services/oauth2/token with refresh_token
   ├─ Get Salesforce access_token
   ├─ Exchange access_token for CDP token at /services/a360/token
   │  └─ Uses grant_type: urn:salesforce:grant-type:external:cdp
   ├─ Get CDP-specific token with special scopes
   └─ Use CDP token to query Data Cloud
```

**Code path:**
```python
# credentials.py
credentials = Credentials.from_ini(profile="default")
# Has: refresh_token, client_id, client_secret

# query_api.py
conn = SalesforceCDPConnection(
    login_url,
    client_id=client_id,
    client_secret=client_secret,
    refresh_token=refresh_token
)

# Inside salesforcecdpconnector:
# 1. POST /services/oauth2/token (refresh token → access token)
# 2. POST /services/a360/token (access token → CDP token)
# 3. Use CDP token for queries
```

**Key Points:**
- ❌ Requires Connected App with Data Cloud scopes
- ❌ Two-step token exchange (OAuth → CDP)
- ❌ Manual credential management
- ❌ Uses salesforcecdpconnector library abstraction

---

### New SF CLI Mechanism (Direct REST API)

**Flow:**
```
1. User authenticates once with SF CLI:
   sf org login web --alias myorg
   (SF CLI stores tokens securely in its own storage)

2. When running code with --sf-org myorg:
   ├─ Call: sf org display --target-org myorg --json
   ├─ Get Salesforce access_token from SF CLI
   ├─ Use access_token DIRECTLY with Data Cloud REST API
   ├─ POST /services/data/v66.0/ssot/query-sql
   └─ No token exchange needed!
```

**Code path:**
```python
# credentials.py
credentials = Credentials.from_sf_cli(org_alias="myorg")
# Runs: sf org display --target-org myorg --json
# Extracts: accessToken, instanceUrl

# sf_cli.py (NEW)
access_token, instance_url = self._get_sf_cli_token()

# Direct REST API call (no salesforcecdpconnector)
response = requests.post(
    f"{instance_url}/services/data/v66.0/ssot/query-sql",
    json={"sql": sql},
    headers={"Authorization": f"Bearer {access_token}"},
    params={"dataspace": dataspace}
)
```

**Key Points:**
- ✅ No Connected App needed
- ✅ No token exchange (direct API access)
- ✅ SF CLI manages token refresh automatically
- ✅ Direct REST API calls (no salesforcecdpconnector)

---

## Key Technical Differences

### 1. **Token Type**

**Original:**
```
refresh_token → OAuth access_token → CDP token (a360)
                                      ↑
                                   Special CDP-scoped token
```

**SF CLI:**
```
SF CLI session token → Direct Data Cloud API access
                       ↑
                    Standard Salesforce API token
```

### 2. **API Endpoint Used**

**Original (salesforcecdpconnector):**
```python
# Uses CDP-specific endpoints
POST /services/a360/token          # Token exchange
POST /services/a360/query          # Query execution (old)
```

**SF CLI (direct REST):**
```python
# Uses standard Salesforce Data Cloud API
POST /services/data/v66.0/ssot/query-sql    # Direct query
```

### 3. **Why SF CLI Token Works Without Exchange**

The SF CLI access token already has permissions to call Data Cloud APIs! It doesn't need the `/services/a360/token` exchange that requires special Connected App scopes.

**The critical discovery was:**

❌ **Original approach tried:** SF CLI token → `/services/a360/token` exchange → **FAILED** (invalid_scope)

✅ **Working approach:** SF CLI token → `/services/data/v66.0/ssot/query-sql` → **SUCCESS**

The Data Cloud REST API (`/ssot/query-sql`) accepts standard Salesforce access tokens, while the CDP token exchange endpoint (`/a360/token`) requires special Connected App scopes that SF CLI tokens don't have.

### 4. **Token Refresh**

**Original:**
```python
# Manually refresh when expired
POST /services/oauth2/token
data = {
    "grant_type": "refresh_token",
    "refresh_token": refresh_token,
    "client_id": client_id,
    "client_secret": client_secret
}
```

**SF CLI:**
```python
# SF CLI handles refresh automatically
# Just fetch fresh token on each call
result = subprocess.run([
    "sf", "org", "display",
    "--target-org", org_alias,
    "--json"
])
# Always get current valid token
```

---

## Files Modified for SF CLI Support

### Core Files (--sf-org flag implementation)

1. **src/datacustomcode/cli.py**
   - Added `--sf-org` flag to `run` and `deploy` commands
   - Routes sf_org to run_entrypoint() and deploy logic

2. **src/datacustomcode/run.py**
   - Accepts `sf_org` parameter
   - Sets `org_alias` in reader/writer configs when `--sf-org` is used

3. **src/datacustomcode/credentials.py**
   - Added `from_sf_cli()` method to fetch credentials from SF CLI
   - Added `AuthType.SF_CLI` enum
   - Added `sf_org_alias` field

4. **src/datacustomcode/io/reader/query_api.py**
   - Added `org_alias` parameter to `__init__()`
   - Routes to `SFCLIDataCloudReader` when `org_alias` is provided
   - Routes to `SalesforceCDPConnection` otherwise

5. **src/datacustomcode/io/writer/print.py**
   - Added `org_alias` parameter
   - Passes to internal reader for schema validation

6. **src/datacustomcode/deploy.py**
   - Updated `_retrieve_access_token()` to handle SF_CLI auth type

### New File

7. **src/datacustomcode/io/reader/sf_cli.py** (NEW)
   - Implements `SFCLIDataCloudReader` class
   - Fetches tokens from SF CLI using subprocess
   - Makes direct REST API calls to Data Cloud
   - No dependency on salesforcecdpconnector

---

## Why This Matters

By bypassing the CDP connector library and using direct REST API calls, we can use SF CLI tokens without any special configuration. This provides a much simpler developer experience:

**Before:**
- Create Connected App
- Configure OAuth scopes
- Get refresh token
- Store in credentials.ini
- Hope tokens don't expire

**After:**
- `sf org login web --alias myorg`
- `datacustomcode run ./payload/entrypoint.py --sf-org myorg`
- Done! ✨
