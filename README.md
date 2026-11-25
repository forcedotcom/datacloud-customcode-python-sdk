# Data 360 Code Extension SDK

This package is the development kit for [Data 360 Code Extensions](https://developer.salesforce.com/docs/data/data-cloud-code-ext/guide/use-custom-code.html). It allows you to write your own data processing logic in Python while leveraging Data 360's infrastructure for data access and running data transformations, mapping execution into data structures like [Data Model Objects](https://help.salesforce.com/s/articleView?id=data.c360_a_data_model_objects.htm&type=5) and [Data Lake Objects](https://help.salesforce.com/s/articleView?id=sf.c360_a_data_lake_objects.htm&language=en_US&type=5).

More specifically, this codebase gives you ability to test code locally before pushing to Data 360's remote execution engine, greatly reducing how long it takes to develop.

Use of this project with Salesforce is subject to the [TERMS OF USE](./TERMS_OF_USE.md)

## Prerequisites

- **Python 3.11 only** (currently supported version - if your system version is different, we recommend using [pyenv](https://github.com/pyenv/pyenv) to configure 3.11)
- JDK 17
- Docker support like [Docker Desktop](https://docs.docker.com/desktop/)
- A salesforce org with some DLOs or DMOs with data and this feature enabled
- the [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli) and [data-code-extension plugin](https://github.com/salesforcecli/plugin-data-code-extension)

## Installation
The SDK can be downloaded directly from PyPI with `pip`:
```
pip install salesforce-data-customcode
```

You can verify it was properly installed via CLI:
```
datacustomcode version
```

## Quick start
Ensure you have all the [prerequisites](#prerequisites) prepared on your machine.

To get started, create a directory and initialize a new project with the CLI:
```zsh
mkdir datacloud && cd datacloud
python3.11 -m venv .venv
source .venv/bin/activate
pip install salesforce-data-customcode
sf data-code-extension script init -p my_package
```

To create a package of type function, use `sf data-code-extension function init`

This will yield all necessary files to get started:
```
.
├── Dockerfile
├── README.md
├── requirements.txt
├── requirements-dev.txt
├── payload
│   ├── config.json
│   ├── entrypoint.py
├── jupyterlab.sh
└── requirements.txt
```
* `Dockerfile` <span style="color:grey;font-style:italic;">(Do not update)</span> – Development container emulating the remote execution environment.
* `requirements-dev.txt` <span style="color:grey;font-style:italic;">(Do not update)</span> – These are the dependencies for the development environment.
* `jupyterlab.sh` <span style="color:grey;font-style:italic;">(Do not update)</span> – Helper script for setting up Jupyter.
* `requirements.txt` – Here you define the requirements that you will need for your script.
* `payload` – This folder will be compressed and deployed to the remote execution environment.
  * `config.json` – This config defines permissions on the back and can be generated programmatically with `scan` CLI method.
  * `entrypoint.py` – The script that defines the data transformation logic.

A functional entrypoint.py is provided so you can run once you've configured authention with your org:
```zsh
cd my_package
sf data-code-extension script run -e ./payload/entrypoint.py -o myorg
```

> [!IMPORTANT]
> The example entrypoint.py requires a `Account_std__dll` DLO to be present.  And in order to deploy the script (next step), the output DLO (which is `Account_std_copy__dll` in the example entrypoint.py) also needs to exist and be in the same dataspace as `Account_std__dll`.

After modifying the `entrypoint.py` as needed, using any dependencies you add in the `.venv` virtual environment, you can run this script in Data 360:

```zsh
cd my_package
sf data-code-extension script scan -e ./payload/entrypoint.py
sf data-code-extension script deploy -p ./payload --name my_custom_script -d description --cpu-size CPU_L --package-version 1 -o myorg
```

> [!NOTE]
> **CPU Size**: Choose the appropriate CPU/Compute Size based on your workload requirements:
> - **CPU_L / CPU_XL / CPU_2XL / CPU_4XL**: Large, X-Large, 2X-Large and 4X-Large CPU instances for data processing
> - Default is `CPU_2XL` which provides a good balance of performance and cost for most use cases

You can now use the Salesforce Data 360 UI to find the created Data Transform and use the `Run Now` button to run it.
Once the Data Transform run is successful, check the DLO your script is writing to and verify the correct records were added.

## Dependency Management

The SDK automatically handles all dependency packaging for Data 360 deployment. Here's how it works:

1. **Add dependencies to `requirements.txt`** - List any Python packages your script needs
2. **Install locally** - Use `pip install -r requirements.txt` in your virtual environment
3. **Automatic packaging** - When you run `sf data-code-extension script zip`, the SDK automatically:
   - Packages all dependencies from `requirements.txt`
   - Uses the correct platform and architecture for Data 360

**No need to worry about platform compatibility** - the SDK handles this automatically through the Docker-based packaging process.

## files directory

```
.
├── payload
│   ├── config.json
│   ├── entrypoint.py
│   ├── files
│   │   ├── data.csv
```

## py-files directory

Your Python dependencies can be packaged as .py files, .zip archives (containing multiple .py files or a Python package structure), or .egg files.

```
.
├── payload
│   ├── config.json
│   ├── entrypoint.py
│   ├── py-files
│   │   ├── moduleA
│   │   │   ├── __init__.py
│   │   │   ├── moduleA.py
```

## API

Your entry point script will define logic using the `Client` object (for batch transforms) or the `StreamingClient` object (for streaming delta transforms), which wrap the data access layers. Both are singletons; a single transform should use one or the other, not both.

For a batch transform, use `Client`. You should only need the following methods:
* `find_file_path(file_name)` – Resolve a bundled file (placed under `payload/files/`) to a `pathlib.Path` that exists. Works the same locally and inside Data 360 — see [Bundled file resolution](#bundled-file-resolution) below for the full lookup order. Raises `FileNotFoundError` if the file isn't found.
* `read_dlo(name)` – Read from a Data Lake Object by name
* `read_dmo(name)` – Read from a Data Model Object by name
* `write_to_dlo(name, spark_dataframe, write_mode)` – Write to a Data Lake Object by name with a Spark dataframe
* `write_to_dmo(name, spark_dataframe, write_mode)` – Write to a Data Model Object by name with a Spark dataframe

For a streaming (delta) transform, use `StreamingClient`, which exposes the streaming counterparts:
* `read_dlo_deltas()` – Read the streaming change feed (deltas) of a Data Lake Object as a streaming DataFrame.
* `read_dmo_deltas()` – Read the streaming change feed (deltas) of a Data Model Object as a streaming DataFrame.
* `write_dlo_deltas(name, spark_dataframe)` – Write a streaming DataFrame of deltas to a Data Lake Object; returns the started `StreamingQuery`
* `write_dmo_deltas(name, spark_dataframe)` – Write a streaming DataFrame of deltas to a Data Model Object; returns the started `StreamingQuery`

`find_file_path`, `llm_gateway_generate_text`, and `einstein_predict` are available on both clients.

For example:
```python
from datacustomcode import Client

client = Client()

sdf = client.read_dlo('my_DLO')
# some transformations
# ...
client.write_to_dlo('output_DLO')
```

> [!WARNING]
> Currently we only support reading from DMOs and writing to DMOs or reading from DLOs and writing to DLOs, but they cannot mix.

### Writing Currency fields

A DLO or DMO with a Currency field has a system field, `cdp_sys_record_currency__c`, that holds each record's ISO currency code. Data Explorer UI uses it to display currency values, and shows a blank value if it's null (Query Editor still shows it).

Code Extensions don't set `cdp_sys_record_currency__c` for you, since only your script knows what currency its amounts are in. When writing to an object with Currency fields, set it yourself:

```python
from pyspark.sql.functions import lit

# Either source has Currency fields: keep its cdp_sys_record_currency__c in your output
df = source.select("id__c", "amount__c", "cdp_sys_record_currency__c")

# OR amounts are in a known currency
df = df.withColumn("cdp_sys_record_currency__c", lit("EUR"))
```

- Use an ISO code that exists in your org's configured currency rates.
- When aggregating, group by the currency code so amounts in different currencies aren't summed together.

See [`docs/examples/script/currency_totals`](docs/examples/script/currency_totals) for a complete example.

### Bundled file resolution

Place bundled files (CSVs, prompt files, etc.) under `payload/files/`. The same `client.find_file_path("data.csv")` call resolves consistently across all three runtimes:

- `datacustomcode run` (local) → `<cwd>/payload/files/data.csv`
- Data 360 script package → `$LIBRARY_PATH/files/data.csv`
- Data 360 function package → `$LIBRARY_PATH/files/data.csv`

Resolution order (first existing path wins):

1. `$LIBRARY_PATH/files/<file_name>`, then `$LIBRARY_PATH/<file_name>` — when `LIBRARY_PATH` is set. Data 360 sets this for you to the package root.
2. `payload/files/<file_name>` relative to the current working directory.
3. `<config_dir>/files/<file_name>` where `<config_dir>` is the directory of the nearest `config.json` discoverable by walking down from cwd.

If none of these exist, `find_file_path` raises `FileNotFoundError` with the list of paths it tried.

`$LIBRARY_PATH` is set automatically to the root of the package at runtime inside Data 360.

### Generating lineage

`generate_lineage` runs a script entry point locally and derives the field-level `dataLineage` section of `config.json` from the Spark plans of the DataFrames it writes:

```python
from datacustomcode.lineage import generate_lineage

result = generate_lineage("payload/entrypoint.py", sf_cli_org="my-org")
print(result.to_dict()["dataLineage"])
# [{"targetField": "Output__dll.name__c",
#   "sourceFields": ["Input__dll.first__c", "Input__dll.last__c"]}, ...]
```

The entry point runs exactly as it would locally (same config, reader, and credentials), except that writes are traced and validated against the target DLO instead of being performed. It runs in a separate Python process, which inherits your working directory, environment, and `sys.path`; errors raised by the script are re-raised from `generate_lineage`. Reads use the reader's `default_row_limit` (1000 by default; set it in your `config.yaml`).

> **Review the output before you use it.** The generated lineage is a starting point for review, not a guarantee. It reflects only the code paths and data your local run exercised, and it can't see inside UDFs. Accurate and complete `dataLineage` in your package is your responsibility as the package author.

- A target field's sources are the fields its *value* is computed from. Fields that only decide which rows, groups or branch are used aren't sources, however the logic is written: filter conditions, join keys, `groupBy` keys, sort keys, window `partitionBy`/`orderBy` keys, `when()`/`if()` conditions, and aggregate `FILTER (WHERE …)` clauses. For example, `groupBy("region").agg(sum("amount"))` and `sum("amount").over(Window.partitionBy("region"))` both trace to `amount` only.
- Fields computed only from literals are listed in `noSources`. So are fields whose value doesn't come from any input field's value, such as `row_number()`, `rank()`, `count("*")`, or `sum(when(cond, 1))`.
- Fields that can't be traced are listed in `unresolved` with a reason and get no lineage. This covers data that didn't come from `read_dlo`/`read_dmo` (for example a `toPandas()` → `createDataFrame` round trip), `mapInPandas`, and subqueries.
- `pairCount` is the total number of (target, source) field pairs.
- Only batch script packages are supported.


## CLI (Deprecated)

The command-line interface (CLI) bundled with the SDK has been deprecated. Users must use the [SF CLI plugin](https://github.com/salesforcecli/plugin-data-code-extension) instead.

## Testing LLM Gateway

You can use AI models configured in Salesforce to generate responses while transforming your data. Below is a sample code example:

```
from datacustomcode.client import Client, llm_gateway_generate_text_col


def main():
  client = Client()
  df = client.read_dlo("Input__dll")
  # llm_gateway_generate_text_col returns a struct
  # {status, response, error_code, error_message} per row, so per-row
  # failures don't abort the Spark job. Pick the field you want with [].
  df_generated = df.withColumn(
    "greeting__c",
    llm_gateway_generate_text_col(
        "In one sentence, greet {name} from {city}.",
        {"name": col("name__c"), "city": col("homecity__c")},
        model_id="sfdc_ai__DefaultGPT4Omni", # An AI model in your org
    )["response"],
  )

  dlo_name = "Output_dll"
  client.write_to_dlo(dlo_name, df_upper1, write_mode=WriteMode.APPEND)

  greeting = client.llm_gateway_generate_text("In one sentence, generate a greeting message", "sfdc_ai__DefaultGPT52")

if __name__ == "__main__":
  main()
```

In order to test this code on your local machine before deploying it to Data 360, you must first set up an External Client App that allows access to the Agent API. Follow this guide to create the ECA https://developer.salesforce.com/docs/ai/agentforce/guide/agent-api-get-started.html#create-a-salesforce-app. You must use `http://localhost:1717/OauthRedirect` as the callback URL.

Once the ECA is set up, log in to your org using this ECA
```
sf org login web \
  --alias myorg \
  --instance-url https://{MY_DOMAIN_URL} \
  --client-id {CONSUMER_KEY} \
  --scopes "sfap_api api"
```

then you can test your code using `myorg` alias
```
sf data-code-extension script run -e ./payload/entrypoint.py -o myorg
```

## Testing Einstein Predictions

You can use AI models configured in Einstein Studio to score your data while
transforming it. As with the LLM Gateway, there are two flavors: a one-shot
scalar call (`client.einstein_predict`) and a per-row column helper
(`einstein_predict_col`). Below is a sample code example:

```
from datacustomcode.client import Client, einstein_predict_col
from datacustomcode.einstein_predictions.types import PredictionType


def main():
  client = Client()
  df = client.read_dlo("Input__dll")
  # einstein_predict_col returns a struct
  # {status, response, error_code, error_message} per row, so per-row
  # failures don't abort the Spark job. `response` is the prediction
  # payload as a JSON string. Pick the field you want with [].
  df_scored = df.withColumn(
    "prediction__c",
    einstein_predict_col(
        "my_regression_model",  # An AI model in your org
        PredictionType.REGRESSION,
        {"square_feet": col("square_feet__c"), "beds": col("beds__c")},
    )["response"],
  )

  dlo_name = "Output_dll"
  client.write_to_dlo(dlo_name, df_scored, write_mode=WriteMode.APPEND)

  # One-shot scalar prediction returns the response payload as a dict
  prediction = client.einstein_predict(
    "my_regression_model",
    PredictionType.REGRESSION,
    {"square_feet": 1800, "beds": 3},
  )

if __name__ == "__main__":
  main()
```

Testing this code locally uses the same External Client App setup described in
[Testing LLM Gateway](#testing-llm-gateway). Once your `myorg` alias is set up,
run:
```
sf data-code-extension script run -e ./payload/entrypoint.py -o myorg
```

## Docker usage

The SDK provides Docker-based development options that allow you to test your code in an environment that closely resembles Data 360's execution environment.

### How Docker Works with the SDK

When you initialize a project with `datacustomcode init my_package`, a `Dockerfile` is created automatically. This Dockerfile:

- **Isn't used during local development** with virtual environments
- **Becomes active during packaging** when you run `datacustomcode zip` or `deploy`
- **Ensures compatibility** by using the same base image as Data 360
- **Handles dependencies automatically** regardless of platform differences

### VS Code Dev Containers

Within your `init`ed package, you will find a `.devcontainer` folder which allows you to run a docker container while developing inside of it.

Read more about Dev Containers here: https://code.visualstudio.com/docs/devcontainers/containers.
#### Setup Instructions

1. Install the VS Code extension "Dev Containers" by microsoft.com.
2. Open your package folder in VS Code, ensuring that the `.devcontainer` folder is
at the root of the File Explorer
3. Bring up the Command Palette (on mac: Cmd + Shift + P), and select "Dev
Containers: Rebuild and Reopen in Container"
4. Allow the docker image to be built, then you're ready to develop

#### Development Workflow

Once inside the Dev Container:
- **Terminal access**: Open a terminal within the container
- **Run your code**: Execute `datacustomcode run ./payload/entrypoint.py`
- **Environment consistency**: Your code will run inside a docker container that more closely resembles Data 360 compute than your machine

> [!TIP]
> **IDE Configuration**: Use `CMD+Shift+P` (or `Ctrl+Shift+P` on Windows/Linux), then select "Python: Select Interpreter" to configure the correct Python Interpreter

> [!IMPORTANT]
> Dev Containers get their own tmp file storage, so you'll need to re-run `datacustomcode configure` every time you "Rebuild and Reopen in Container".

### JupyterLab

Within your `init`ed package, you will find a `jupyterlab.sh` file that can open a jupyter notebook for you.  Jupyter notebooks, in
combination with Data 360's [Query Editor](https://help.salesforce.com/s/articleView?id=data.c360_a_add_queries_to_a_query_workspace.htm&type=5)
and [Data Explorer](https://help.salesforce.com/s/articleView?id=data.c360_a_data_explorer.htm&type=5), can be extremely helpful for data
exploration.  Instead of running an entire script, one can run one code cell at a time as they discover and experiment with the DLO or DMO data.

You can read more about Jupyter Notebooks here: https://jupyter.org/

1. Within the root project of your package folder, run `./jupyterlab.sh start`. This prints an access token and opens an already-authenticated JupyterLab session in your browser. If the browser doesn't open automatically, copy the printed `http://localhost:8888/?token=...` URL into your browser.
1. Double-click on "account.ipynb" file, which provides a starting point for a notebook
1. Use shift+enter to execute each cell within the notebook.  Add/edit/delete cells of code as needed for your data exploration.
1. Don't forget to run `./jupyterlab.sh stop` to stop the docker container

> [!IMPORTANT]
> JupyterLab uses its own tmp file storage, so you'll need to re-run `datacustomcode configure` each time you `./jupyterlab.sh start`.

## Prerequisite details

### Creating an External Client app

1. Log in to Salesforce as an admin. In the top right corner, click on the gear icon and go to `Setup`
2. On the left sidebar, expand `Apps`, expand `External Client Apps`, click `Settings`
3. Expand `Apps`, expand `External Client Apps`, click `External Client App Manager`
4. Click `New External Client App` button
5. Fill in the required fields within the `Basic Information` section
6. Under the `API (Enable OAuth Settings)` section:
    1. Click on the checkbox to Enable OAuth Settings
    2. Provide a callback URL like `http://localhost:5555/callback`
    3. In the Selected OAuth Scopes, make sure that `refresh_token`, `api`, `cdp_query_api`, `cdp_profile_api` are selected
    4. Check the following:
        - Enable Authorization Code and Credentials Flow
        - Require user credentials in the POST body for Authorization Code and Credentials Flow
    5. Uncheck `Require Proof Key for Code Exchange (PKCE) extension for Supported Authorization Flows`
    6. Click on `Create` button
7. On your newly created External Client App page, on the `Policies` tab:
    1. In the `App Authorization` section, choose an appropriate Refresh Token Policy as per your expected usage and preference.
    2. Under `App Authorization`, set IP Relaxation to `Relax IP restrictions` unless otherwise needed
8. Click `Save`
9. Go to the `Settings` tab, under `OAuth Settings`. There, you can click on the `Consumer Key and Secret` button which will open a new tab. There you can copy the `client_id` and `client_secret` values which are to be used during configuring credentials using this SDK.
10. Logout
11. Use the URL of the login page as the `login_url` value when setting up the SDK

You now have all fields necessary for the `datacustomcode configure` command.

### Using the Salesforce CLI for authentication

The [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli) (`sf`) lets you authenticate an org once and then reference it by alias across tools.

#### Installing the Salesforce CLI

Follow the [official install guide](https://developer.salesforce.com/docs/atlas.en-us.sfdx_setup.meta/sfdx_setup/sfdx_setup_install_cli.htm), or use a package manager:

Verify the install:
```zsh
sf --version
```

#### Authenticating an org

**Browser-based (recommended for developer orgs and sandboxes):**
```zsh
# Production / Developer Edition
sf org login web --alias myorg --instance-url <your-instance-url>

# Sandbox
sf org login web --alias mysandbox --instance-url https://test.salesforce.com

# Custom domain
sf org login web --alias myorg --instance-url https://mycompany.my.salesforce.com
```

Each command opens a browser tab. After you log in and approve access, the CLI stores the session locally.

**Verify the stored org and confirm the alias:**
```zsh
sf org list
sf org display --target-org myorg
```

### Obtaining Refresh Token and Core Token

If you're using OAuth Tokens authentication, the initial configure will retrieve and store tokens. Run `datacustomcode auth` to refresh these when they expire.

## Other docs

- [Troubleshooting](./docs/troubleshooting.md)
- [Contributing](./CONTRIBUTING.md)
