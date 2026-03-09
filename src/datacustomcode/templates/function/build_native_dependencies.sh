#!/bin/bash
set -e

# Description: build native dependencies for function (unpacked pip install to py-files)

python3.11 -m venv --copies .venv
source .venv/bin/activate
pip install --target ./py-files -r requirements.txt
