.DEFAULT_GOAL := help

# Directory variables
SRC_DIR := src/
TEST_DIR := tests/

# Automatically generate help text from comments
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all ## Run all commands
all: install check test

.PHONY: fmt
fmt: ## Run ruff formatter on all Python files
	uv run ruff format $(SRC_DIR) $(TEST_DIR)

.PHONY: lint
lint: ## Run ruff linter with autofix (including unsafe fixes)
	uv run ruff check --fix --unsafe-fixes $(SRC_DIR) $(TEST_DIR)

.PHONY: types
types: ## Run pyright type checker in strict mode
	uv run pyright $(SRC_DIR) $(TEST_DIR)

.PHONY: test
test: ## Run pytest on all test files
	uv run pytest $(TEST_DIR)

.PHONY: check
check: fmt lint types ## Run all checks (format, lint, type checking)

.PHONY: install
install: ## Set up the project with dependencies and pre-commit hooks
	uv sync --upgrade
	uv run pre-commit autoupdate
	uv run pre-commit install
	uv run pre-commit install --hook-type pre-push

.PHONY: install-ci
install-ci: ## Install dependencies from the lockfile for CI
	uv sync --frozen

.PHONY: update-games
update-games: ## Update basketball game data from Covers.com
	uv run python -m point_spreads.multi_parser

.PHONY: clean
clean: ## Clean up temporary files and caches
	rm -rf uv.lock
	rm -rf .cache
	rm -rf .ruff_cache
	rm -rf .pytest_cache
	rm -rf .venv
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type f -name ".coverage.*" -delete
	make install
