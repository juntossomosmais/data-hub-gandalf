#!/usr/bin/env bash

# Lint, format, and type-check gandalf. Adopts the tinuvi convention
# (ruff --fix + ruff format + pyrefly), scoped to gandalf's source and tests.
#
# ruff config + pyrefly config live in pyproject.toml. Deps are system-installed
# (POETRY_VIRTUALENVS_CREATE=false), so the tools are called directly.
set -e

# Mark the bind-mounted repo as a safe git directory (no-op outside the container).
git config --global --add safe.directory /app 2>/dev/null || true

ruff check --fix --exit-non-zero-on-fix gandalf/ tests/
ruff format --exit-non-zero-on-fix gandalf/ tests/
pyrefly check
