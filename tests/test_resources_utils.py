import os
import re
import unittest
from pathlib import Path
from unittest import mock

from tests.resources import utils
from tests.resources.utils import _databricks_connect_configured

# Infrastructure identifiers that must never be hardcoded in the test helpers: an Azure Databricks
# workspace host (adb-<workspace-id>.<n>.azuredatabricks.net) and a Databricks cluster id
# (NNNN-NNNNNN-xxxxxx). These are matched against the helper's source to prevent re-introduction.
_WORKSPACE_HOST_RE = re.compile(r"adb-\d+\.\d+\.azuredatabricks\.net", re.IGNORECASE)
_CLUSTER_ID_RE = re.compile(r"\b\d{4}-\d{6}-[a-z0-9]{6,}\b", re.IGNORECASE)


class NoLeakedInfraIdentifiersTest(unittest.TestCase):
    """Guards against re-introducing the leaked Databricks workspace host / cluster id."""

    def test_source_has_no_workspace_host_or_cluster_id(self):
        # Arrange
        source = Path(utils.__file__).read_text()

        # Act
        host_matches = _WORKSPACE_HOST_RE.findall(source)
        cluster_matches = _CLUSTER_ID_RE.findall(source)

        # Assert
        self.assertEqual(host_matches, [], f"workspace host hardcoded in utils.py: {host_matches}")
        self.assertEqual(cluster_matches, [], f"cluster id hardcoded in utils.py: {cluster_matches}")


class DatabricksConnectConfiguredTest(unittest.TestCase):
    """The databricks-connect path is opt-in via standard Databricks config; with nothing
    configured the shipped Spark Connect server is used instead."""

    def test_not_configured_when_no_databricks_env(self):
        # Arrange / Act / Assert
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(_databricks_connect_configured())

    def test_configured_when_host_set(self):
        # Arrange / Act / Assert
        with mock.patch.dict(os.environ, {"DATABRICKS_HOST": "https://example.cloud.databricks.com"}, clear=True):
            self.assertTrue(_databricks_connect_configured())

    def test_configured_when_profile_set(self):
        # Arrange / Act / Assert
        with mock.patch.dict(os.environ, {"DATABRICKS_CONFIG_PROFILE": "my-profile"}, clear=True):
            self.assertTrue(_databricks_connect_configured())


if __name__ == "__main__":
    unittest.main()
