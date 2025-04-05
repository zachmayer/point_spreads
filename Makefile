.DEFAULT_GOAL := help

# Directory variables
SRC_DIR := src/

# Automatically generate help text from comments
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all ## Run all commands
all: install check

.PHONY: fmt
fmt: ## Run ruff formatter on all Python files
	ruff format $(SRC_DIR)

.PHONY: lint
lint: ## Run ruff linter with autofix (including unsafe fixes)
	ruff check --fix --unsafe-fixes $(SRC_DIR)

.PHONY: types
types: ## Run pyright type checker in strict mode
	uv run pyright $(SRC_DIR)

.PHONY: check
check: fmt lint types ## Run all checks (format, lint, type checking)

.PHONY: install
install: ## Set up the project with dependencies and pre-commit hooks
	uv pip install -e .
	uv run pre-commit install
	uv run pre-commit install --hook-type pre-push

.PHONY: update
update: ## Update dependencies and pre-commit hooks
	uv pip install -e . --upgrade
	uv run pre-commit autoupdate
