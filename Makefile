# Freemarket Medallion pipeline — single-command entrypoints.
# Assumes an activated virtualenv (see SETUP.md): python3 -m venv .venv && source .venv/bin/activate
.PHONY: install pipeline render test all clean slides screenshot

# Chrome binary for the slides/screenshot targets (override: make slides CHROME=...)
CHROME ?= /Applications/Google Chrome.app/Contents/MacOS/Google Chrome

install:            ## Install pinned dependencies
	python -m pip install -r requirements.txt

pipeline:           ## Build the warehouse (Bronze -> Silver -> Gold)
	python -m src.pipeline

render:             ## Render the illustrative star map (optional proof) -> submission/star_map.html
	python -m src.render

test:               ## Run the test suite
	pytest

slides:             ## Rebuild submission/slides.pdf from slides/slides.html (needs Chrome)
	"$(CHROME)" --headless --disable-gpu --print-to-pdf=submission/slides.pdf \
		--no-pdf-header-footer "file://$(CURDIR)/slides/slides.html"

screenshot:         ## Recapture slides/star_map_screenshot.png from submission/star_map.html
	"$(CHROME)" --headless --disable-gpu --screenshot=slides/star_map_screenshot.png \
		--window-size=1560,790 --hide-scrollbars --virtual-time-budget=15000 \
		"file://$(CURDIR)/submission/star_map.html"

all: pipeline test  ## Build the pipeline, then run the tests

clean:              ## Remove the built warehouse
	rm -f submission/warehouse.duckdb submission/warehouse.duckdb.wal
