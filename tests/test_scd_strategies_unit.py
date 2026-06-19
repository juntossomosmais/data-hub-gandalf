"""Strategy unit tests — require PySpark (run in Databricks Connect or local Spark).

These are pure mock-based tests: no live Spark session or Delta table is used.
In CI/Databricks they provide coverage of the SCD strategy dispatch logic.
"""

import unittest
from unittest.mock import MagicMock, patch

import pyspark  # noqa: F401  (ensures PySpark is importable in this environment)

from gandalf.scd.config import SCDConfig
from gandalf.scd.strategies import scd2_cdc_merge, scd4_merge


class TestScd2CdcRaisesNotImplemented(unittest.TestCase):
    """scd2_cdc must raise NotImplementedError (not silently no-op) — rule #6."""

    def test_raises_not_implemented_when_called_directly(self):
        # Act / Assert
        with self.assertRaisesRegex(NotImplementedError, "scd2_cdc"):
            scd2_cdc_merge()

    def test_raises_not_implemented_with_args(self):
        # Act / Assert
        with self.assertRaisesRegex(NotImplementedError, "scd2_cdc"):
            scd2_cdc_merge(MagicMock(), MagicMock(), ["id"], "path")

    def test_error_message_is_actionable(self):
        # Act
        with self.assertRaises(NotImplementedError) as exc_info:
            scd2_cdc_merge()

        # Assert
        msg = str(exc_info.exception)
        self.assertIn("scd2_cdc", msg)
        self.assertGreater(len(msg), 20, "error message should explain why, not just name the type")


class TestScd4MergeNoOpHistory(unittest.TestCase):
    """scd4_merge must not append duplicate history rows when checksum is unchanged."""

    def _config(self):
        return SCDConfig(
            scd_type="scd4",
            ids=["id"],
            history_path="catalog.schema.hist",
            history_path_type="table",
        )

    def _target_df_mock(self, has_rows: bool, has_checksum: bool = True):
        target_df = MagicMock()
        target_df.limit.return_value.count.return_value = 1 if has_rows else 0
        target_df.columns = ["id", "nome", "scd_checksum"] if has_checksum else ["id", "nome"]
        return target_df

    def _source_df_mock(self):
        df = MagicMock()
        df.columns = ["id", "nome", "scd_checksum"]
        enriched = MagicMock()
        enriched.columns = ["id", "nome", "scd_checksum", "scd_start_dt", "scd_end_dt"]
        df.withColumn.return_value = enriched
        enriched.withColumn.return_value = enriched
        return df, enriched

    def test_initial_load_appends_all_rows_to_history(self):
        # Arrange
        config = self._config()
        target_df = self._target_df_mock(has_rows=False)
        dt_mock = MagicMock()
        dt_mock.toDF.return_value = target_df
        source_df, enriched = self._source_df_mock()

        # Act
        with (
            patch("gandalf.scd.strategies._get_delta_table", return_value=dt_mock),
            patch("gandalf.scd.strategies._write_df") as mock_write,
            patch("gandalf.scd.strategies.scd1_merge") as mock_scd1,
        ):
            scd4_merge(MagicMock(), source_df, ["id"], "path", "table", config=config)

        # Assert
        mock_write.assert_called_once()
        self.assertEqual(mock_write.call_args[0][1], "catalog.schema.hist")
        self.assertEqual(mock_write.call_args.kwargs["mode"], "append")
        mock_scd1.assert_called_once()

    def test_no_op_update_writes_filtered_df_to_history(self):
        """On repeated load with same data, only the changed_or_new result reaches history."""
        # Arrange
        config = self._config()
        target_df = self._target_df_mock(has_rows=True, has_checksum=True)
        dt_mock = MagicMock()
        dt_mock.toDF.return_value = target_df
        source_df, enriched = self._source_df_mock()

        # Simulate: enriched.join(...).where(...).drop(...) = changed_df
        changed_df = MagicMock()
        join_chain = MagicMock()
        join_chain.where.return_value.drop.return_value = changed_df
        enriched.join.return_value = join_chain

        # target_df.select(...).withColumnRenamed(...) must return something with a join
        target_df.select.return_value.withColumnRenamed.return_value = MagicMock()

        # Act
        with (
            patch("gandalf.scd.strategies._get_delta_table", return_value=dt_mock),
            patch("gandalf.scd.strategies._write_df") as mock_write,
            patch("gandalf.scd.strategies.scd1_merge"),
        ):
            scd4_merge(MagicMock(), source_df, ["id"], "path", "table", config=config)

        # Assert
        # History write must use the filtered result, not the full enriched df
        mock_write.assert_called_once()
        self.assertIs(mock_write.call_args[0][0], changed_df)
        self.assertEqual(mock_write.call_args[0][1], "catalog.schema.hist")

    def test_target_without_checksum_falls_back_to_full_append(self):
        """If target has no checksum column, fall back to appending all rows (safe default)."""
        # Arrange
        config = self._config()
        target_df = self._target_df_mock(has_rows=True, has_checksum=False)
        dt_mock = MagicMock()
        dt_mock.toDF.return_value = target_df
        source_df, enriched = self._source_df_mock()

        # Act
        with (
            patch("gandalf.scd.strategies._get_delta_table", return_value=dt_mock),
            patch("gandalf.scd.strategies._write_df") as mock_write,
            patch("gandalf.scd.strategies.scd1_merge"),
        ):
            scd4_merge(MagicMock(), source_df, ["id"], "path", "table", config=config)

        # Assert
        mock_write.assert_called_once()
        self.assertIs(mock_write.call_args[0][0], enriched)


if __name__ == "__main__":
    unittest.main()
