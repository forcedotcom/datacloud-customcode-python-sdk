#!/bin/bash

# Description: build native dependencies

pip3.11 install venv-pack

python3.11 -m venv --copies .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -o native_dependencies.tar.gz -f
