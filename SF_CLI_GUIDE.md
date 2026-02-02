# Data Cloud Custom Code SDK - SF CLI Guide

This guide shows you how to use the Data Cloud Custom Code SDK with Salesforce CLI authentication.

## Prerequisites

1. **Python 3.11** installed
2. **Salesforce CLI** installed and authenticated
3. **Data Cloud Custom Code SDK** installed

### Install Salesforce CLI

```bash
npm install -g @salesforce/cli
```

### Authenticate with Salesforce CLI

```bash
# Login to your Salesforce production org
sf org login web --alias myorg
# for OrgFarm org:
sf org login web --alias myorg -r orgfarm_org_url 
for example : sf org login web --alias myorg -r https://orgfarm-ebc419d8e7.test1.my.pc-rnd.salesforce.com/ 

# Verify authentication
sf org display --target-org myorg
```

## Installation

```bash
# Install from source
git clone <repository-url>
cd datacloud-customcode-python-sdk
python3.11 -m venv .venv
source .venv/bin/activate
pip install poetry
poetry install
```

## Project Setup

### Initialize a New Project

```bash
# Create a script-based project
datacustomcode init my-project --code-type script

# Or create a function-based project
datacustomcode init my-project --code-type function

cd my-project
```

This creates:
```
my-project/
├── payload/
│   ├── entrypoint.py    # Your transformation code
│   ├── config.json      # Data Cloud permissions
│   └── requirements.txt # Python dependencies
├── Dockerfile
└── .devcontainer/
```

## Development Workflow

### 1. Write Your Transformation

Edit `payload/entrypoint.py`:

```python
from datacustomcode import Client
from datacustomcode.io.writer.base import WriteMode

def main():
    client = Client()

    # Read from Data Lake Object (DLO)
    df = client.read_dlo("Account_Home__dll")

    # Transform data
    df_transformed = df.filter(df.Industry == "Technology")

    # Write to another DLO
    client.write_to_dlo(
        "Account_Filtered__dll",
        df_transformed,
        write_mode=WriteMode.OVERWRITE
    )

if __name__ == "__main__":
    main()
```

### 2. Scan for Permissions

```bash
# Auto-generate config.json with required permissions
datacustomcode scan payload/entrypoint.py
```

This updates `payload/config.json` with the DLOs/DMOs your code accesses.

### 3. Test Locally

```bash
# Run locally using SF CLI authentication
datacustomcode run ./payload/entrypoint.py --sf-org myorg
```

**What happens:**
- Fetches fresh token from SF CLI automatically
- Connects to Data Cloud using your authenticated org
- Executes your transformation locally
- Prints output to console (doesn't write to Data Cloud)

### 4. Deploy to Data Cloud

```bash
# Package and deploy to Data Cloud
datacustomcode deploy \
  --name my-transform \
  --sf-org myorg \
  --path payload
```

**Optional deploy parameters:**
```bash
datacustomcode deploy \
  --name my-transform \
  --version 1.0.0 \
  --description "My data transformation" \
  --cpu-size CPU_2XL \
  --sf-org myorg \
  --path payload
```

**Available CPU sizes:**
- `CPU_L` - Large
- `CPU_XL` - X-Large
- `CPU_2XL` - 2X-Large (default)
- `CPU_4XL` - 4X-Large

**For function-based projects:**
```bash
datacustomcode deploy \
  --name my-function \
  --function-invoke-opt "Account_Home__dll,Account_Filtered__dll" \
  --sf-org myorg \
  --path payload
```

## Working with Multiple Orgs

You can easily switch between different SF CLI authenticated orgs:

```bash
# Run against org1
datacustomcode run ./payload/entrypoint.py --sf-org org1

# Run against org2
datacustomcode run ./payload/entrypoint.py --sf-org org2

# Deploy to production
datacustomcode deploy --name my-transform --sf-org production
```

## Common Commands

### List Available Orgs

```bash
# See all authenticated SF CLI orgs
sf org list
```

### Run with Different Dataspace

The dataspace is automatically read from `payload/config.json`. To change it, edit the config file:

```json
{
  "dataspace": "my_dataspace",
  "type": "script",
  "permissions": [...]
}
```

### Package Without Deploying

```bash
# Create deployment package
datacustomcode zip payload
```

This creates a Docker image with your code and dependencies.

## Reading Data

### Read from Data Lake Objects (DLOs)

```python
client = Client()

# Read entire DLO (limited by row_limit)
df = client.read_dlo("Account_Home__dll")

# Default row_limit is 1000 for local testing
df = client.read_dlo("Account_Home__dll", row_limit=5000)
```

### Read from Data Model Objects (DMOs)

```python
# Read from DMO
df = client.read_dmo("Account__dlm")
```

**Important:** You cannot mix DLOs and DMOs. Either read/write DLOs only, or read/write DMOs only.

## Writing Data

### Write Modes

```python
from datacustomcode.io.writer.base import WriteMode

# Overwrite existing data
client.write_to_dlo("Output__dll", df, WriteMode.OVERWRITE)

# Append to existing data
client.write_to_dlo("Output__dll", df, WriteMode.APPEND)

# Error if data already exists
client.write_to_dlo("Output__dll", df, WriteMode.ERRORIFEXISTS)
```

## File Access

```python
client = Client()

# Find file in payload directory
file_path = client.find_file_path("data.csv")

# Read file
with open(file_path) as f:
    data = f.read()
```

Files must be placed in the `py-files` directory within your payload folder.

## Troubleshooting

### SF CLI Token Issues

```bash
# Refresh authentication
sf org login web --alias myorg

# Check token status
sf org display --target-org myorg --json
```

### Common Errors

**"table does not exist"**
- The DLO/DMO doesn't exist in Data Cloud
- Create the object first or use an existing one

**"Mixing DLOs and DMOs"**
- You read from a DLO but tried to write to a DMO (or vice versa)
- Keep reads and writes within the same object type

**"SF CLI command not found"**
- Install Salesforce CLI: `npm install -g @salesforce/cli`

**"No access token"**
- Authenticate first: `sf org login web --alias myorg`

## Testing

Run the test suite:

```bash
# Run all tests
make test

# Run specific tests
poetry run pytest tests/test_client.py
```

## Advanced Configuration

### Custom Dependencies

Add Python packages to `payload/requirements.txt`:

```txt
pandas==2.0.0
numpy==1.24.0
```

The SDK will automatically package them during deployment.

### Environment-Specific Config

You can maintain different configs for different environments:

```bash
# Development
datacustomcode run ./payload/entrypoint.py --sf-org dev-org

# Staging
datacustomcode run ./payload/entrypoint.py --sf-org staging-org

# Production
datacustomcode run ./payload/entrypoint.py --sf-org prod-org
```

## Best Practices

1. **Always run `scan` after changing your code** to update permissions in config.json
2. **Test locally first** with `run` before deploying
3. **Use descriptive names** for your transformations
4. **Version your deployments** with meaningful version numbers
5. **Keep transformations focused** - one transformation should do one thing well
6. **Handle errors gracefully** in your code
7. **Use appropriate CPU sizes** based on your data volume

## Example: Complete Workflow

```bash
# 1. Setup
sf org login web --alias myorg
datacustomcode init employee-transform --code-type script
cd employee-transform

# 2. Develop
# Edit payload/entrypoint.py with your transformation logic

# 3. Update permissions
datacustomcode scan payload/entrypoint.py

# 4. Test locally
datacustomcode run ./payload/entrypoint.py --sf-org myorg

# 5. Deploy
datacustomcode deploy \
  --name employee-transform \
  --version 1.0.0 \
  --sf-org myorg \
  --path payload

# 6. Iterate
# Make changes to entrypoint.py
datacustomcode scan payload/entrypoint.py
datacustomcode run ./payload/entrypoint.py --sf-org myorg
datacustomcode deploy --name employee-transform --version 1.1.0 --sf-org myorg
```

## Support

For issues or questions:
- Check the main README.md
- Review test files in `tests/` directory for examples
- Check SDK documentation in source code

## What's Next?

- Explore the template examples in `templates/`
- Review the test suite for advanced usage patterns
- Check out function-based transformations for event-driven processing
