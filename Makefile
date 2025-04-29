.PHONY: clean install develop lint package test mypy
RM := rm -rf

clean:
	$(RM) $(filter-out .venv, $(wildcard *egg-info .coverage .mypy_cache .pytest_cache \
		.tox reports .ruff_cache))
	find . -name '__pycache__' -not -path './.venv/*' | xargs $(RM)
	find . -name '.ipynb_checkpoints' -not -path './.venv/*' | xargs $(RM)

install:
	poetry install --only main

develop:
	poetry install --with dev

lint:
	poetry run pre-commit run --all-files --hook-stage manual

mypy:
	poetry run mypy src/

package:
	$(RM) dist/
	poetry build

test: develop
	poetry run pytest
