#!/bin/bash
set -e

# Description: build native dependencies

python3.11 -m venv --copies .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -o native_dependencies.tar.gz -f
