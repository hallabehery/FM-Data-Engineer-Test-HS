# Freemarket Medallion pipeline — single-command entrypoints.
# Assumes an activated virtualenv (see SETUP.md): python3 -m venv .venv && source .venv/bin/activate
.PHONY: install pipeline render test all clean

install:            ## Install pinned dependencies
	python -m pip install -r requirements.txt

pipeline:           ## Build the warehouse (Bronze -> Silver -> Gold)
	python -m src.pipeline

render:             ## Render the illustrative star map (optional proof) -> submission/star_map.html
	python -m src.render

test:               ## Run the test suite
	pytest

all: pipeline test  ## Build the pipeline, then run the tests

clean:              ## Remove the built warehouse
	rm -f submission/warehouse.duckdb submission/warehouse.duckdb.wal
