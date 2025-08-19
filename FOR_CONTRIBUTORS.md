# New Developer Guide - Data Cloud Custom Code Python SDK

Welcome to the Salesforce Data Cloud Custom Code Python SDK! This guide will help you get started with development and contribution to this repository.

## 🚀 Quick Start

### Prerequisites

See the [Prerequisites section in README.md](./README.md#prerequisites) for complete setup requirements.

### Initial Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd datacloud-customcode-python-sdk
   ```

2. **Set up virtual environment and install Poetry**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install poetry
   poetry build
   ```

3. **Install dependencies**
   ```bash
   # Install main dependencies
   poetry install --only main
   
   # Install development dependencies
   poetry install --with dev
   ```

4. **Verify installation**
   ```bash
   poetry run datacustomcode version
   ```

## 🔧 Makefile Commands

The project includes a comprehensive Makefile for common development tasks:

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

---

**Welcome to the community!** If you have any questions or need help getting started, don't hesitate to create an issue in the repository or reach out to the maintainers through the project's communication channels.
