# Freemarket Medallion pipeline — single-command entrypoints.
# Assumes an activated virtualenv (see SETUP.md): python3 -m venv .venv && source .venv/bin/activate
.PHONY: install pipeline test all clean

install:            ## Install pinned dependencies
	python -m pip install -r requirements.txt

pipeline:           ## Build the warehouse (Bronze -> Silver -> Gold)
	python -m src.pipeline

test:               ## Run the test suite
	pytest

all: pipeline test  ## Build the pipeline, then run the tests

clean:              ## Remove the built warehouse
	rm -f submission/warehouse.duckdb submission/warehouse.duckdb.wal
