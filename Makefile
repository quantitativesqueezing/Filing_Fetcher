PY?=python3
VENV?=.venv
PIP:=$(VENV)/bin/pip
PYBIN:=$(VENV)/bin/python

URLS?=

.PHONY: venv install run ndjson

venv:
	@test -d $(VENV) || $(PY) -m venv $(VENV)

install: venv
	$(PIP) install -r requirements.txt

run: install
	$(PYBIN) scripts/fetch_sec_docs.py $(URLS)

ndjson: run
	@true
