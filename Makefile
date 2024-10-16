VENV = poetry run

# Линтеры
lint: mypy ruff

mypy:
	$(VENV) mypy .

ruff:
	$(VENV) ruff check --no-cache .
