from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gandalf.scd.config import SCDConfig
from gandalf.utils.df_validations import (
    validate_no_duplicate_current,
    validate_required_columns,
    validate_scd_source_columns,
    validate_scd_target_columns,
)
from tests.resources.utils import PySparkTestCase


class TestDfValidationsScd(PySparkTestCase):
    def test_validate_required_columns_fails_with_missing_columns(self):
        # Arrange
        df = self.spark.createDataFrame([("1",)], StructType([StructField("id", StringType())]))
        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            validate_required_columns(df, ["id", "name"], "source")
        self.assertIn("source missing required columns", str(ctx.exception))

    def test_validate_scd_source_columns_checks_ids_and_tracked_columns(self):
        # Arrange
        df = self.spark.createDataFrame([("1",)], StructType([StructField("id", StringType())]))
        config = SCDConfig(scd_type="scd1", ids=["id"], tracked_columns=["name"])
        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            validate_scd_source_columns(df, config)
        self.assertIn("source missing required columns", str(ctx.exception))

    def test_validate_scd_target_columns_requires_scd2_control_columns(self):
        # Arrange
        df = self.spark.createDataFrame([("1",)], StructType([StructField("id", StringType())]))
        config = SCDConfig(scd_type="scd2", ids=["id"])
        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            validate_scd_target_columns(df, config)
        self.assertIn("target missing required columns", str(ctx.exception))

    def test_validate_scd_target_columns_requires_scd3_previous_columns(self):
        # Arrange
        df = self.spark.createDataFrame(
            [("1", "Alice")],
            StructType([StructField("id", StringType()), StructField("name", StringType())]),
        )
        config = SCDConfig(scd_type="scd3", ids=["id"], previous_columns={"name": "previous_name"})
        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            validate_scd_target_columns(df, config)
        self.assertIn("previous_name", str(ctx.exception))

    def test_validate_no_duplicate_current_fails_when_current_key_is_duplicated(self):
        # Arrange
        schema = StructType([StructField("id", StringType()), StructField("current_flag", IntegerType())])
        df = self.spark.createDataFrame([("1", 1), ("1", 1), ("1", 0)], schema)
        # Act / Assert
        with self.assertRaises(Exception):
            validate_no_duplicate_current(df, ["id"], "current_flag")
