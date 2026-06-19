import unittest

from gandalf.scd.columns import SCDColumns
from gandalf.scd.config import SCDConfig
from gandalf.scd.constants import (
    SCD0,
    SCD1,
    SCD1_DELETE,
    SCD2,
    SCD2_CDC,
    SCD2_NO_DELETE,
    SCD3,
    SCD4,
    SCD6,
    normalize_scd_type,
)


class TestScdTypeAliases(unittest.TestCase):
    def test_normalize_scd_type_accepts_legacy_and_canonical_names(self):
        # Arrange
        cases = [
            ("scd", SCD2),
            ("scd_no_delete", SCD2_NO_DELETE),
            ("upsert", SCD1),
            ("upsert-delete", SCD1_DELETE),
            ("scd0", SCD0),
            ("scd1", SCD1),
            ("scd1_delete", SCD1_DELETE),
            ("scd2", SCD2),
            ("scd2_no_delete", SCD2_NO_DELETE),
            ("scd2_cdc", SCD2_CDC),
            ("scd3", SCD3),
            ("scd4", SCD4),
            ("scd6", SCD6),
        ]

        for value, expected in cases:
            with self.subTest(value=value, expected=expected):
                # Act
                result = normalize_scd_type(value)

                # Assert
                self.assertEqual(result, expected)

    def test_invalid_type_raises_value_error_with_allowed_values(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "Invalid merge_type") as ctx:
            normalize_scd_type("wizard")

        message = str(ctx.exception)
        self.assertIn("scd2", message)
        self.assertIn("upsert-delete", message)


class TestScdConfigValidation(unittest.TestCase):
    def test_ids_are_required(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "ids"):
            SCDConfig(scd_type="scd2", ids=[])

    def test_scd_type_is_normalized(self):
        # Act
        config = SCDConfig(scd_type="scd", ids=["id"])

        # Assert
        self.assertEqual(config.scd_type, SCD2)

    def test_scd4_requires_history_path(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "history_path"):
            SCDConfig(scd_type="scd4", ids=["id"])

    def test_scd3_requires_previous_columns(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "previous_columns"):
            SCDConfig(scd_type="scd3", ids=["id"])

    def test_scd6_requires_previous_columns(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "previous_columns"):
            SCDConfig(scd_type="scd6", ids=["id"])

    def test_scd0_requires_protected_or_tracked_columns(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "protected_columns"):
            SCDConfig(scd_type="scd0", ids=["id"])

    def test_scd2_cdc_requires_timestamp_column(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "cdc_timestamp_col"):
            SCDConfig(scd_type="scd2_cdc", ids=["id"], cdc_timestamp_col="")

    def test_previous_columns_cannot_collide_with_ids(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "collide"):
            SCDConfig(scd_type="scd3", ids=["id"], previous_columns={"name": "id"})

    def test_from_legacy_preserves_checksum_ignore_list_and_alias(self):
        # Act
        config = SCDConfig.from_legacy(
            merge_type="scd_no_delete",
            ids=["id"],
            checksum_ignore_list=["updated_at"],
        )

        # Assert
        self.assertEqual(config.scd_type, SCD2_NO_DELETE)
        self.assertEqual(config.ids, ["id"])
        self.assertEqual(config.checksum_ignore_columns, ["updated_at"])

    def test_from_legacy_builds_scd0_with_protected_columns(self):
        # Act
        config = SCDConfig.from_legacy(
            merge_type="scd0",
            ids=["id"],
            protected_columns=["nome"],
        )

        # Assert
        self.assertEqual(config.scd_type, SCD0)
        self.assertEqual(config.protected_columns, ["nome"])

    def test_from_legacy_builds_scd3_with_previous_columns(self):
        # Act
        config = SCDConfig.from_legacy(
            merge_type="scd3",
            ids=["id"],
            previous_columns={"email": "previous_email"},
        )

        # Assert
        self.assertEqual(config.scd_type, SCD3)
        self.assertEqual(config.previous_columns, {"email": "previous_email"})

    def test_from_legacy_builds_scd4_with_history_path(self):
        # Act
        config = SCDConfig.from_legacy(
            merge_type="scd4",
            ids=["id"],
            history_path="catalog.schema.history",
        )

        # Assert
        self.assertEqual(config.scd_type, SCD4)
        self.assertEqual(config.history_path, "catalog.schema.history")

    def test_from_legacy_scd0_without_protected_columns_fails(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "protected_columns"):
            SCDConfig.from_legacy(merge_type="scd0", ids=["id"])

    def test_from_legacy_scd3_without_previous_columns_fails(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "previous_columns"):
            SCDConfig.from_legacy(merge_type="scd3", ids=["id"])

    def test_from_legacy_scd4_without_history_path_fails(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "history_path"):
            SCDConfig.from_legacy(merge_type="scd4", ids=["id"])


class TestScdConfigColumns(unittest.TestCase):
    def test_default_columns_are_scd_namespaced(self):
        # Act
        config = SCDConfig(scd_type="scd2", ids=["id"])

        # Assert
        self.assertEqual(config.columns, SCDColumns())
        self.assertEqual(config.effective_from_col, "scd_start_dt")
        self.assertEqual(config.effective_to_col, "scd_end_dt")
        self.assertEqual(config.current_flag_col, "scd_is_current")
        self.assertEqual(config.checksum_col, "scd_checksum")

    def test_custom_columns_reflected_in_properties(self):
        # Arrange
        columns = SCDColumns(is_current="current_flag", checksum="check_sum")

        # Act
        config = SCDConfig(scd_type="scd2", ids=["id"], columns=columns)

        # Assert
        self.assertEqual(config.current_flag_col, "current_flag")
        self.assertEqual(config.checksum_col, "check_sum")
        self.assertEqual(config.effective_from_col, "scd_start_dt")

    def test_from_legacy_carries_columns(self):
        # Arrange
        columns = SCDColumns(checksum="hash_n")

        # Act
        config = SCDConfig.from_legacy(merge_type="scd", ids=["id"], columns=columns)

        # Assert
        self.assertEqual(config.checksum_col, "hash_n")

    def test_non_scdcolumns_columns_raises_type_error(self):
        # Act / Assert
        with self.assertRaises(TypeError):
            SCDConfig(scd_type="scd2", ids=["id"], columns={"is_current": "current_flag"})

    def test_control_column_colliding_with_id_raises(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "collide with ids"):
            SCDConfig(scd_type="scd2", ids=["scd_is_current"])

    def test_control_column_colliding_with_previous_column_raises(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "collide with previous_columns"):
            SCDConfig(scd_type="scd3", ids=["id"], previous_columns={"nome": "scd_checksum"})
