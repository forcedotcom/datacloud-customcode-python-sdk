# Contributing Guide For datacloud-customcode-python-sdk

This page lists the operational governance model of this project, as well as the recommendations and requirements for how to best contribute to datacloud-customcode-python-sdk. We strive to obey these as best as possible. As always, thanks for contributing – we hope these guidelines make it easier and shed some light on our approach and processes.

# Governance Model

The intent and goal of open sourcing this project is because it may contain useful or interesting code/concepts that we wish to share with the larger open source community. Although occasional work may be done on it, we will not be looking for or soliciting contributions.

# Issues, requests & ideas

Use GitHub Issues page to submit issues, enhancement requests and discuss ideas.

### Bug Reports and Fixes
-  If you find a bug, please search for it in the [Issues](https://github.com/atacloud-customcode-python-sdk/issues), and if it isn't already tracked,
   [create a new issue](https://github.com/datacloud-customcode-python-sdk/issues/new). Fill out the "Bug Report" section of the issue template. Even if an Issue is closed, feel free to comment and add details, it will still
   be reviewed.
-  Issues that have already been identified as a bug (note: able to reproduce) will be labelled `bug`.

### New Features
-  If you'd like new functionality added to this project, describe the problem you want to solve in a [new Issue](https://github.com/datacloud-customcode-python-sdk/issues/new).
-  Issues that have been identified as a feature request will be labelled `enhancement`.


### Issues
We use GitHub issues to track public bugs. Please ensure your description is
clear and has sufficient instructions to be able to reproduce the issue.

# Code of Conduct
Please follow our [Code of Conduct](CODE_OF_CONDUCT.md).

# Development

## Quick Start

### Prerequisites

See the [Prerequisites section in README.md](./README.md#prerequisites) for complete setup requirements.

### Initial Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd datacloud-customcode-python-sdk
   ```

2. **Set up virtual environment and install dependencies**

   **Note**: If you need to set a specific Python version, use `pyenv local 3.11.x` in the project directory.

   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   pip install poetry
   make develop
   ```

3. **Verify installation**
   ```bash
   datacustomcode version
   ```

4. **Initialize a project for development work verification**

   **Note**: To test your changes and develop new features, initialize a sample project:

   ```bash
   # Create a new directory for your test project
   mkdir my-test-project
   cd my-test-project

   # Initialize a new Data Cloud custom code project
   datacustomcode init .

   # Test your SDK modifications against the sample project with:
   datacustomcode run ./payload/entrypoint.py
   ```

   **Tip**: See the [README.md](./README.md) for additional `datacustomcode` commands (`scan`, `deploy`, `zip`) to test specific code paths and validate your SDK changes thoroughly.

## Makefile Commands

```bash
# Clean build artifacts, caches and temporary files
make clean

# Build package distribution
make package

# Install main dependencies only
make install

# Install dependencies for full development setup
make develop

# Run code quality checks
make lint

# Perform static type checking
make mypy

# Run complete test suite
make test
```
