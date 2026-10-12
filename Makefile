PYTHON ?= python
VENV := .venv
ifeq ($(OS),Windows_NT)
PY := $(VENV)/Scripts/python.exe
RUFF := $(VENV)/Scripts/ruff.exe
else
PY := $(VENV)/bin/python
RUFF := $(VENV)/bin/ruff
endif

.PHONY: setup lint fmt test up down bench demo clean migrate api receiver
setup:
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install -r requirements-lock.txt -e ".[dev]"
lint:
	$(RUFF) check .
	$(RUFF) format --check .
fmt:
	$(RUFF) check --fix .
	$(RUFF) format .
test:
	$(PY) -m pytest --cov=relay --cov=receiver --cov-report=term-missing
up:
	docker compose up -d --wait
	$(PY) -m relay.db
migrate:
	$(PY) -m relay.db
down:
	docker compose down
bench:
	$(PY) -m bench.run $(BENCH_ARGS)
demo:
	$(PY) scripts/demo.py
api:
	$(PY) -m uvicorn relay.api:app --host 127.0.0.1 --port 8101
receiver:
	$(PY) -m uvicorn receiver.mock_receiver:app --host 127.0.0.1 --port 8102
clean:
	$(PYTHON) -c "from pathlib import Path; import shutil; [shutil.rmtree(p, ignore_errors=True) for p in [Path('.pytest_cache'), Path('.ruff_cache'), Path('htmlcov'), Path('build'), Path('dist')]]"
