#!/usr/bin/env bash

# Starts a Delta-enabled Spark Connect server so gandalf's integration tests
# (tests/conftest.py -> SparkSession.builder.remote("sc://...")) can run against a real
# Spark + Delta backend inside Docker. Writes "1" to a flag file once the server is up so
# the docker-compose healthcheck can gate the integration-tests service on service_healthy.
#
# Delta version coupling: the delta-connect-server package, the deltaio/delta-docker base tag,
# and the client delta-spark pin (pyproject.toml) must all be on the same Delta line. Only the
# unsuffixed 4.0.0 delta-connect-server coordinate is published on Maven, so this stays on 4.0.0.
#
# Note: plain `set -e` (NOT pipefail) — the log-polling pipelines below intentionally tolerate
# a non-zero `ls` while the log file is still being created.
set -e

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
LOG_DIR="${SPARK_HOME}/logs"
CUSTOM_FLAG_FILE="${CUSTOM_FLAG_FILE:-${LOG_DIR}/custom-is-server-up.txt}"

DELTA_CONNECT_PACKAGE="${DELTA_CONNECT_PACKAGE:-io.delta:delta-connect-server_2.13:4.0.0}"
PROTOBUF_PACKAGE="${PROTOBUF_PACKAGE:-com.google.protobuf:protobuf-java:3.25.1}"

# Total seconds to wait for the server to come up (cold Ivy/Maven resolution of the Delta
# Connect jars can take a while on first boot).
STARTUP_DEADLINE_SECONDS="${STARTUP_DEADLINE_SECONDS:-180}"

mkdir -p "${LOG_DIR}"

# Mark the server as NOT up yet so the healthcheck stays unhealthy until startup is detected.
echo "0" > "${CUSTOM_FLAG_FILE}"

echo "##### Starting Spark Connect server (packages: ${DELTA_CONNECT_PACKAGE},${PROTOBUF_PACKAGE})"

# start-connect-server.sh launches the server in the background and returns.
"${SPARK_HOME}/sbin/start-connect-server.sh" \
  --packages "${DELTA_CONNECT_PACKAGE},${PROTOBUF_PACKAGE}" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.extensions.relation.classes=org.apache.spark.sql.connect.delta.DeltaRelationPlugin" \
  --conf "spark.connect.extensions.command.classes=org.apache.spark.sql.connect.delta.DeltaCommandPlugin" \
  --conf "spark.authenticate=false"

PATTERN="spark-*org.apache.spark.sql.connect.service.SparkConnectServer-*.out"
START_MSG="INFO SparkConnectServer: Spark Connect server started at"

deadline=$(( $(date +%s) + STARTUP_DEADLINE_SECONDS ))

# 1) Wait for the Connect server log file to appear.
logfile=""
while :; do
  logfile=$(ls -t "${LOG_DIR}"/${PATTERN} 2>/dev/null | head -n1 || true)
  if [[ -n "${logfile}" ]]; then
    break
  fi
  if (( $(date +%s) >= deadline )); then
    echo "##### ERROR: Spark Connect log file not found in '${LOG_DIR}' within ${STARTUP_DEADLINE_SECONDS}s."
    exit 1
  fi
  sleep 1
done

# 2) Watch the log for the startup banner; flip the flag file to "1" when seen.
while :; do
  if grep -q "${START_MSG}" "${logfile}" 2>/dev/null; then
    echo "1" > "${CUSTOM_FLAG_FILE}"
    echo "##### Spark Connect server is UP (banner detected in ${logfile})."
    break
  fi
  if (( $(date +%s) >= deadline )); then
    echo "##### ERROR: Did not detect Spark Connect server startup banner within ${STARTUP_DEADLINE_SECONDS}s."
    echo "##### ----- tail of ${logfile} -----"
    tail -n 80 "${logfile}" || true
    exit 1
  fi
  sleep 1
done

echo "##### Following Spark Connect log: ${logfile}"
# Keep the container alive (and surface server logs) by following the log file.
exec tail -F "${logfile}"
