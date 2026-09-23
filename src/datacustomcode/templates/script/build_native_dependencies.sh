#!/bin/bash
set -e

# Description: build native dependencies

python3.11 -m venv --copies .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -o native_dependencies.tar.gz -f

# Reset file ownership within workspace folder
# to match host user
chown -R "$(stat -c '%u:%g' /workspace)" /workspace
