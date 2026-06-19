import unittest

from gandalf.scd.predicates import (
    append_extra_condition,
    build_current_key_condition,
    build_key_condition,
    quote_identifier,
    validate_identifier,
)


class TestScdPredicates(unittest.TestCase):
    def test_validate_identifier_accepts_simple_names(self):
        # Arrange
        name = "id_client"

        # Act
        result = validate_identifier(name)

        # Assert
        self.assertEqual(result, "id_client")

    def test_validate_identifier_rejects_unsafe_names(self):
        # Arrange
        unsafe_names = ["1id", "id-client", "id client", "id;drop", "target.id"]

        # Act / Assert
        for name in unsafe_names:
            with self.subTest(name=name):
                with self.assertRaises(ValueError) as ctx:
                    validate_identifier(name)
                self.assertIn("Unsafe SQL identifier", str(ctx.exception))

    def test_quote_identifier_wraps_valid_name_in_backticks(self):
        # Arrange
        name = "id"

        # Act
        result = quote_identifier(name)

        # Assert
        self.assertEqual(result, "`id`")

    def test_build_key_condition_supports_composite_keys(self):
        # Act
        result = build_key_condition("target", "source", ["id", "tenant_id"])

        # Assert
        self.assertEqual(
            result,
            "target.`id` = source.`id` AND target.`tenant_id` = source.`tenant_id`",
        )

    def test_build_current_key_condition_adds_current_flag(self):
        # Act
        result = build_current_key_condition("target", "staged", ["id"], "current_flag")

        # Assert
        self.assertEqual(
            result,
            "target.`id` = staged.`id` AND target.`current_flag` = 1",
        )

    def test_append_extra_condition_adds_parentheses(self):
        # Act
        result = append_extra_condition("target.`id` = source.`id`", "source.dt >= '2024-01-01'")

        # Assert
        self.assertEqual(
            result,
            "target.`id` = source.`id` AND (source.dt >= '2024-01-01')",
        )

    def test_append_extra_condition_ignores_empty_extra(self):
        # Act
        result = append_extra_condition("base", "")

        # Assert
        self.assertEqual(result, "base")
