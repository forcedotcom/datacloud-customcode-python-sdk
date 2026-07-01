# Data Cloud Custom Code SDK

This package provides a development kit for creating custom data transformations in [Data Cloud](https://www.salesforce.com/data/). It allows you to write your own data processing logic in Python while leveraging Data Cloud's infrastructure for data access and running data transformations, mapping execution into Data Cloud data structures like [Data Model Objects](https://help.salesforce.com/s/articleView?id=data.c360_a_data_model_objects.htm&type=5) and [Data Lake Objects](https://help.salesforce.com/s/articleView?id=sf.c360_a_data_lake_objects.htm&language=en_US&type=5).

More specifically, this codebase gives you ability to test code locally before pushing to Data Cloud's remote execution engine, greatly reducing how long it takes to develop.

Use of this project with Salesforce is subject to the [TERMS OF USE](./TERMS_OF_USE.md)

## Prerequisites

- **Python 3.11 only** (currently supported version - if your system version is different, we recommend using [pyenv](https://github.com/pyenv/pyenv) to configure 3.11)
- JDK 17
- Docker support like [Docker Desktop](https://docs.docker.com/desktop/)
- A salesforce org with some DLOs or DMOs with data and this feature enabled
- **One of the following** for authentication:
  - A Salesforce org already authenticated via the [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli)
    (simplest — no External Client App needed)
  - An [External Client App](#creating-an-external-client-app) configured with OAuth settings

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
datacustomcode init my_package
```

To create a package of type function, pass the parameter `--code-type=function` with the init command.

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

A functional entrypoint.py is provided so you can run once you've configured your External Client App:
```zsh
cd my_package
datacustomcode configure
datacustomcode run ./payload/entrypoint.py
```

> [!TIP]
> **Already using the Salesforce CLI?** If you have authenticated an org with `sf org login web
> --alias myorg`, you can skip `datacustomcode configure` entirely:
> ```zsh
> datacustomcode run ./payload/entrypoint.py --sf-cli-org myorg
> ```

> [!IMPORTANT]
> The example entrypoint.py requires a `Account_std__dll` DLO to be present.  And in order to deploy the script (next step), the output DLO (which is `Account_std_copy__dll` in the example entrypoint.py) also needs to exist and be in the same dataspace as `Account_std__dll`.

After modifying the `entrypoint.py` as needed, using any dependencies you add in the `.venv` virtual environment, you can run this script in Data Cloud:

**To Add New Dependencies**:
1. Make sure your virtual environment is activated
2. Add dependencies to `requirements.txt`
3. Run `pip install -r requirements.txt`
4. The SDK automatically packages all dependencies when you run `datacustomcode zip`

```zsh
cd my_package
datacustomcode scan ./payload/entrypoint.py
datacustomcode deploy --path ./payload --name my_custom_script --cpu-size CPU_L --sf-cli-org myorg
```

> [!TIP]
> The `deploy` process can take several minutes.  If you'd like more feedback on the underlying process, you can add `--debug` to the command like `datacustomcode --debug deploy --path ./payload --name my_custom_script`

> [!NOTE]
> **CPU Size**: Choose the appropriate CPU/Compute Size based on your workload requirements:
> - **CPU_L / CPU_XL / CPU_2XL / CPU_4XL**: Large, X-Large, 2X-Large and 4X-Large CPU instances for data processing
> - Default is `CPU_2XL` which provides a good balance of performance and cost for most use cases

You can now use the Salesforce Data Cloud UI to find the created Data Transform and use the `Run Now` button to run it.
Once the Data Transform run is successful, check the DLO your script is writing to and verify the correct records were added.

## Dependency Management

The SDK automatically handles all dependency packaging for Data Cloud deployment. Here's how it works:

1. **Add dependencies to `requirements.txt`** - List any Python packages your script needs
2. **Install locally** - Use `pip install -r requirements.txt` in your virtual environment
3. **Automatic packaging** - When you run `datacustomcode zip`, the SDK automatically:
   - Packages all dependencies from `requirements.txt`
   - Uses the correct platform and architecture for Data Cloud

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

Your entry point script will define logic using the `Client` object which wraps data access layers.

You should only need the following methods:
* `find_file_path(file_name)` – Resolve a bundled file (placed under `payload/files/`) to a `pathlib.Path` that exists. Works the same locally and inside Data Cloud — see [Bundled file resolution](#bundled-file-resolution) below for the full lookup order. Raises `FileNotFoundError` if the file isn't found.
* `read_dlo(name)` – Read from a Data Lake Object by name
* `read_dmo(name)` – Read from a Data Model Object by name
* `write_to_dlo(name, spark_dataframe, write_mode)` – Write to a Data Model Object by name with a Spark dataframe
* `write_to_dmo(name, spark_dataframe, write_mode)` – Write to a Data Lake Object by name with a Spark dataframe

For streaming (delta) transforms, the streaming counterparts are:
* `read_dlo_deltas(name)` – Read the streaming change feed (deltas) of a Data Lake Object as a streaming DataFrame
* `read_dmo_deltas(name)` – Read the streaming change feed (deltas) of a Data Model Object as a streaming DataFrame
* `write_dlo_deltas(name, spark_dataframe, write_mode)` – Write a streaming DataFrame of deltas to a Data Lake Object; returns the started `StreamingQuery`

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

### Streaming (delta) transforms

Streaming BYOC transforms process a Data Lake Object's Change Data Feed continuously instead of reading a bounded snapshot. Use the `*_deltas` methods in place of the batch read/write methods:

```python
from pyspark.sql.functions import col, upper

from datacustomcode import Client
from datacustomcode.io.writer.base import WriteMode

client = Client()

# read_dlo_deltas returns a *streaming* DataFrame over the change feed.
deltas = client.read_dlo_deltas("Input__dll")

# Ordinary PySpark transform. Keep the change-feed metadata columns
# (those starting with "_") — the streaming sink needs them to apply
# inserts, updates, and deletes to the target DLO.
transformed = deltas.withColumn("description__c", upper(col("description__c")))

# write_dlo_deltas starts a streaming query and returns the StreamingQuery.
# The runtime owns the trigger and checkpoint location; you choose only the
# target table and write mode.
query = client.write_dlo_deltas("Output__dll", transformed, WriteMode.APPEND)
query.awaitTermination()
```

Notes:

- Supported streaming write modes are `WriteMode.APPEND`, `WriteMode.OVERWRITE`, and `WriteMode.MERGE_UPSERT_DELETE`.
- These methods only run inside the Data Cloud streaming (`DELTA_SYNC`) runtime. Locally (`datacustomcode run`) they raise `NotImplementedError`, since there is no change feed to stream.
- A complete runnable entry point is provided in [`examples/streaming_deltas/entrypoint.py`](src/datacustomcode/templates/script/examples/streaming_deltas/entrypoint.py).

### Bundled file resolution

Place bundled files (CSVs, prompt files, etc.) under `payload/files/`. The same `client.find_file_path("data.csv")` call resolves consistently across all three runtimes:

- `datacustomcode run` (local) → `<cwd>/payload/files/data.csv`
- Data Cloud script package → `$LIBRARY_PATH/files/data.csv`
- Data Cloud function package → `$LIBRARY_PATH/files/data.csv`

Resolution order (first existing path wins):

1. `$LIBRARY_PATH/files/<file_name>`, then `$LIBRARY_PATH/<file_name>` — when `LIBRARY_PATH` is set. Data Cloud sets this for you to the package root.
2. `payload/files/<file_name>` relative to the current working directory.
3. `<config_dir>/files/<file_name>` where `<config_dir>` is the directory of the nearest `config.json` discoverable by walking down from cwd.

If none of these exist, `find_file_path` raises `FileNotFoundError` with the list of paths it tried.

`$LIBRARY_PATH` is set automatically to the root of the package at runtime inside Data Cloud.


## CLI

The Data Cloud Custom Code SDK provides a command-line interface (CLI) with the following commands:

### Global Options
- `--debug`: Enable debug-level logging

### Commands

#### `datacustomcode version`
Display the current version of the package.

#### `datacustomcode configure`
Configure credentials for connecting to Data Cloud.

**Prerequisites:**
- An [External Client App](#creating-an-external-client-app) with OAuth settings configured
- For OAuth Tokens authentication: [refresh token and core token](#obtaining-refresh-token-and-core-token)

Options:
- `--profile TEXT`: Credential profile name (default: "default")
- `--auth-type TEXT`: Authentication method (default: `oauth_tokens`)
  - `oauth_tokens` - OAuth tokens with refresh_token
  - `client_credentials` - Server-to-server using client_id/secret only

You will be prompted for the following depending on auth type:

*Common to all auth types:*
- **Login URL**: Salesforce login URL
- **Client ID**: External Client App Client ID

*For OAuth Tokens authentication:*
- **Client Secret**: External Client App Client Secret
- **Redirect URI**: OAuth redirect URI

*For Client Credentials authentication:*
- **Client Secret**: External Client App Client Secret

##### Using Environment Variables (Alternative)

Instead of using `datacustomcode configure`, you can also set credentials via environment variables.

> [!NOTE]
> Environment variables take precedence over the credentials INI file when both are present.

**Common (required for all auth types):**
| Variable | Description |
|----------|-------------|
| `SFDC_LOGIN_URL` | Salesforce login URL (e.g., `https://login.salesforce.com`) |
| `SFDC_CLIENT_ID` | External Client App Client ID |
| `SFDC_AUTH_TYPE` | Authentication type: `oauth_tokens` (default) or `client_credentials` |

**For OAuth Tokens authentication (`SFDC_AUTH_TYPE=oauth_tokens`):**
| Variable | Description |
|----------|-------------|
| `SFDC_CLIENT_SECRET` | External Client App Client Secret |
| `SFDC_REFRESH_TOKEN` | OAuth refresh token |
| `SFDC_ACCESS_TOKEN` | (Optional) OAuth core/access token |

**Einstein Platform API Environment (Optional):**
| Variable | Description |
|----------|-------------|
| `SFDC_EINSTEIN_API_ENV` | Einstein Platform API environment: `dev`, `test`, `stage`, or `prod`. If not set, automatically inferred from login URL. Set this explicitly if auto-detection fails. |

Example usage:
```bash
export SFDC_LOGIN_URL="https://login.salesforce.com"
export SFDC_CLIENT_ID="your_client_id"
export SFDC_CLIENT_SECRET="your_client_secret"
export SFDC_REFRESH_TOKEN="your_refresh_token"
export SFDC_EINSTEIN_API_ENV="test"  # optional

datacustomcode run ./payload/entrypoint.py
```


#### `datacustomcode init`
Initialize a new development environment with a code package template.

Argument:
- `DIRECTORY`: Directory to create project in (default: ".")
Options:
- `--code-type TEXT`: This can be either `function` or `script`. The default value is `script` if the argument is missing.


#### `datacustomcode scan`
Scan a Python file to generate a Data Cloud configuration.

Argument:
- `FILENAME`: Python file to scan

Options:
- `--config TEXT`: Path to save the configuration file (default: same directory as FILENAME)
- `--dry-run`: Preview the configuration without saving to a file


#### `datacustomcode run`
Run an entrypoint file locally for testing.

Argument:
- `ENTRYPOINT`: Path to entrypoint Python file

Options:
- `--config-file TEXT`: Path to configuration file
- `--dependencies TEXT`: Additional dependencies (can be specified multiple times)
- `--profile TEXT`: Credential profile name (default: "default")
- `--sf-cli-org TEXT`: Salesforce CLI org alias or username (e.g. `dev1`). Fetches
  credentials via `sf org display` — no `datacustomcode configure` step needed.
  Takes precedence over `--profile` if both are supplied.


#### `datacustomcode zip`
Zip a transformation job in preparation to upload to Data Cloud. Make sure to change directory into your code package folder (e.g., `my_package`) before running this command.

Arguments:
- `PATH`: Path to the code directory i.e. the payload folder (default: "payload")

Options:
- `--network TEXT`: docker network (default: "default")


#### `datacustomcode deploy`
Deploy a transformation job to Data Cloud. Note that this command takes care of creating a zip file from provided path before deployment. Make sure to change directory into your code package folder (e.g., `my_package`) before running this command.

Options:
- `--profile TEXT`: Credential profile name (default: "default")
- `--path TEXT`: Path to the code directory i.e. the payload folder (default: ".")
- `--name TEXT`: Name of the transformation job [required]
- `--version TEXT`: Version of the transformation job (default: "0.0.1")
- `--description TEXT`: Description of the transformation job (default: "")
- `--network TEXT`: docker network (default: "default")
- `--cpu-size TEXT`: CPU size for the deployment (default: `CPU_2XL`). Available options: CPU_L(Large), CPU_XL(Extra Large), CPU_2XL(2X Large), CPU_4XL(4X Large)
- `--sf-cli-org TEXT`: Salesforce CLI org alias or username (e.g. `myorg`). Fetches credentials via `sf org display` — no `datacustomcode configure` step needed. Takes precedence over `--profile` if both are supplied.
- `--function-invoke-opt TEXT`: Currently we support only `UnstructuredChunking` for functions.


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

In order to test this code on your local machine before deploying it to Data Cloud, you must first set up an External Client App that allows access to the Agent API. Follow this guide to create the ECA https://developer.salesforce.com/docs/ai/agentforce/guide/agent-api-get-started.html#create-a-salesforce-app. You must use `http://localhost:1717/OauthRedirect` as the callback URL.

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
datacustomcode run ./payload/entrypoint.py --sf-cli-org myorg
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
datacustomcode run ./payload/entrypoint.py --sf-cli-org myorg
```


## Docker usage

The SDK provides Docker-based development options that allow you to test your code in an environment that closely resembles Data Cloud's execution environment.

### How Docker Works with the SDK

When you initialize a project with `datacustomcode init my_package`, a `Dockerfile` is created automatically. This Dockerfile:

- **Isn't used during local development** with virtual environments
- **Becomes active during packaging** when you run `datacustomcode zip` or `deploy`
- **Ensures compatibility** by using the same base image as Data Cloud
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
- **Environment consistency**: Your code will run inside a docker container that more closely resembles Data Cloud compute than your machine

> [!TIP]
> **IDE Configuration**: Use `CMD+Shift+P` (or `Ctrl+Shift+P` on Windows/Linux), then select "Python: Select Interpreter" to configure the correct Python Interpreter

> [!IMPORTANT]
> Dev Containers get their own tmp file storage, so you'll need to re-run `datacustomcode configure` every time you "Rebuild and Reopen in Container".

### JupyterLab

Within your `init`ed package, you will find a `jupyterlab.sh` file that can open a jupyter notebook for you.  Jupyter notebooks, in
combination with Data Cloud's [Query Editor](https://help.salesforce.com/s/articleView?id=data.c360_a_add_queries_to_a_query_workspace.htm&type=5)
and [Data Explorer](https://help.salesforce.com/s/articleView?id=data.c360_a_data_explorer.htm&type=5), can be extremely helpful for data
exploration.  Instead of running an entire script, one can run one code cell at a time as they discover and experiment with the DLO or DMO data.

You can read more about Jupyter Notebooks here: https://jupyter.org/

1. Within the root project of your package folder, run `./jupyterlab.sh start`
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

The [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli) (`sf`) lets you authenticate an org once and then reference it by alias across tools — including this SDK via `--sf-cli-org`.

#### Installing the Salesforce CLI

Follow the [official install guide](https://developer.salesforce.com/docs/atlas.en-us.sfdx_setup.meta/sfdx_setup/sfdx_setup_install_cli.htm), or use a package manager:

```zsh
# macOS (Homebrew)
brew install sf

# npm (all platforms)
npm install --global @salesforce/cli
```

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

Once authenticated, pass the alias directly to `datacustomcode run`:
```zsh
datacustomcode run ./payload/entrypoint.py --sf-cli-org myorg
```

### Obtaining Refresh Token and Core Token

If you're using OAuth Tokens authentication, the initial configure will retrieve and store tokens. Run `datacustomcode auth` to refresh these when they expire.

## Other docs

- [Troubleshooting](./docs/troubleshooting.md)
- [For Contributors](./FOR_CONTRIBUTORS.md)
