.PHONY: build test clean

PYTHON := $(shell if [ -x .venv/bin/python ]; then echo .venv/bin/python; else command -v python3; fi)
XPI := zotero-plugin/dist/zoty-bridge.xpi

build:
	bash zotero-plugin/build.sh

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

clean:
	rm -f $(XPI)
