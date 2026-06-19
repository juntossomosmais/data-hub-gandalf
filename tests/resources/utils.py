import os
import unittest
from uuid import uuid4

from pyspark.sql import SparkSession

# Managed-table namespace for the tests. On Databricks this is a Unity Catalog catalog; against
# the local Spark Connect server, spark_catalog is the built-in session catalog (Hive metastore),
# which supports CREATE SCHEMA + managed Delta tables.
_TEST_CATALOG = os.getenv("GANDALF_TEST_CATALOG", "spark_catalog")
_TEST_SCHEMA = os.getenv("GANDALF_TEST_SCHEMA", "gandalf_it")

# Base path for storage-path (path_type="storage") tests. Resolved on the Spark backend's
# filesystem. Override with an abfss:// path when running against Databricks.
_STORAGE_BASE = os.getenv("GANDALF_TEST_STORAGE_PATH", "/tmp/gandalf-it")


def _databricks_connect_configured() -> bool:
    """True when the environment points databricks-connect at a workspace.

    Host, auth and compute are resolved by the Databricks SDK (config profile / unified auth), so
    we only look for the standard signals that the user configured their *own* workspace: a named
    config profile or an explicit host. With neither set we fall back to the Spark Connect server
    shipped by this repo, so no workspace or cluster identifier is ever baked into the source.
    """
    return bool(os.getenv("DATABRICKS_CONFIG_PROFILE") or os.getenv("DATABRICKS_HOST"))


class PySparkTestCase(unittest.TestCase):
    """Base test case that provides a Spark session for PySpark/Delta tests.

    Connects to *your own* cluster through ``databricks-connect`` when standard Databricks auth is
    configured (``DATABRICKS_CONFIG_PROFILE`` / ``~/.databrickscfg``, or ``DATABRICKS_HOST`` +
    ``DATABRICKS_CLUSTER_ID``; or serverless); otherwise it uses the Spark Connect server shipped by
    this repo, reached via ``CUSTOM_SPARK_REMOTE_HOST``. Subclasses access the session via
    ``self.spark`` (or ``cls.spark``) and use the ``unique_table`` / ``unique_storage_path`` helpers
    for isolated targets.
    """

    spark: SparkSession

    @classmethod
    def setUpClass(cls):
        # getOrCreate reuses the active session across test classes; recreate only if a previous
        # class stopped it.
        if getattr(cls, "spark", None) is None or cls.spark.is_stopped:
            # Use databricks-connect only when the user configured their own workspace AND the
            # client is installed (it is not in the default test image); otherwise the shipped
            # Spark Connect server.
            if _databricks_connect_configured():
                try:
                    cls._use_databricks_connect()
                except ModuleNotFoundError:
                    cls._use_spark_connect()
            else:
                cls._use_spark_connect()
        # The test data is tiny (a handful of rows). Collapse the shuffle fan-out from the default
        # 200 partitions to 1 so each Delta MERGE / aggregation reduce stage runs a single task
        # instead of ~200. spark.sql.shuffle.partitions is a dynamic SQL conf, so it is settable at
        # runtime over Spark Connect (unlike static/core confs such as spark.network.timeout).
        cls.spark.conf.set("spark.sql.shuffle.partitions", "1")
        cls.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_TEST_CATALOG}.{_TEST_SCHEMA}")

    @classmethod
    def _use_spark_connect(cls):
        spark_host = os.getenv("CUSTOM_SPARK_REMOTE_HOST", "localhost:15002")
        cls.spark = SparkSession.builder.remote(f"sc://{spark_host}").appName("gandalf-tests").getOrCreate()

    @classmethod
    def _use_databricks_connect(cls):
        # Host, auth token and compute are resolved by the Databricks SDK from the user's own
        # config (config profile / unified auth / DATABRICKS_* env vars, or serverless). Nothing
        # about a specific workspace or cluster is hardcoded here.
        from databricks.connect import DatabricksSession

        cls.spark = DatabricksSession.builder.getOrCreate()

    def unique_table(self) -> str:
        """Return a unique managed-table identifier in the test schema (no hyphens)."""
        return f"{_TEST_CATALOG}.{_TEST_SCHEMA}.gandalf_{uuid4().hex}"

    def unique_storage_path(self) -> str:
        """Return a unique storage path (``path_type='storage'``) on the Spark backend."""
        return f"{_STORAGE_BASE}/{uuid4().hex}"
