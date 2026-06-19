#!/usr/bin/env bash

# Runs gandalf's full test suite with coverage inside the integration-tests container.
#
# All tests use the session `spark` fixture in tests/conftest.py, which connects to the Spark
# Connect server at sc://${CUSTOM_SPARK_REMOTE_HOST} (or databricks-connect when creds exist).
#
# Deps were installed into the system interpreter (POETRY_VIRTUALENVS_CREATE=false), so the
# tools are invoked directly — no `poetry run` needed.
set -e

REPORTS_FOLDER_PATH="${REPORTS_FOLDER_PATH:-tests-reports}"
TEST_TARGETS="${TEST_TARGETS:-tests/}"

mkdir -p "${REPORTS_FOLDER_PATH}"

echo "##### Running tests against CUSTOM_SPARK_REMOTE_HOST=${CUSTOM_SPARK_REMOTE_HOST:-<unset>}"

# -n auto    : run tests in parallel across CPUs (pytest-xdist). The suite is dominated by gRPC
#              round-trips to the Spark Connect server, so overlapping them cuts wall-clock time.
# --cov      : pytest-cov measures gandalf and aggregates coverage across xdist workers (a plain
#              `coverage run` cannot see the worker subprocesses). The XML is Sonar-compatible.
# --junitxml : test-execution report Sonar consumes (sonar.python.xunit.reportPath).
pytest "${TEST_TARGETS}" \
  -n auto \
  --cov=gandalf \
  --cov-report=term-missing \
  --cov-report=xml:"${REPORTS_FOLDER_PATH}/coverage.xml" \
  --cov-report=html:"${REPORTS_FOLDER_PATH}/html" \
  --junitxml="${REPORTS_FOLDER_PATH}/junit.xml"

echo "##### Coverage written to ${REPORTS_FOLDER_PATH}/coverage.xml"
