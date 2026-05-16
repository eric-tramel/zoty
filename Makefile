.PHONY: build release-build verify-build test clean

PYTHON := $(shell if [ -x .venv/bin/python ]; then echo .venv/bin/python; else command -v python3; fi)
XPI := zotero-plugin/dist/zoty-bridge.xpi
UPDATE_MANIFEST := zotero-plugin/dist/zoty-bridge-updates.json

build:
	bash zotero-plugin/build.sh

release-build:
	test -n "$(RELEASE_TAG)"
	bash zotero-plugin/build.sh --release-tag "$(RELEASE_TAG)" --require-release-tag-match

verify-build: build
	git diff --exit-code -- $(XPI) $(UPDATE_MANIFEST) src/zoty/assets/zoty-bridge.xpi

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

clean:
	rm -f $(XPI) $(UPDATE_MANIFEST)
