import unittest

from gandalf.scd.columns import SCDColumns


class TestScdColumns(unittest.TestCase):
    def test_defaults_are_scd_namespaced(self):
        # Act
        columns = SCDColumns()

        # Assert
        self.assertEqual(columns.start_dt, "scd_start_dt")
        self.assertEqual(columns.end_dt, "scd_end_dt")
        self.assertEqual(columns.is_current, "scd_is_current")
        self.assertEqual(columns.checksum, "scd_checksum")

    def test_as_tuple_and_as_set(self):
        # Arrange
        columns = SCDColumns()

        # Act / Assert
        self.assertEqual(columns.as_tuple(), ("scd_start_dt", "scd_end_dt", "scd_is_current", "scd_checksum"))
        self.assertEqual(columns.as_set(), frozenset(columns.as_tuple()))

    def test_custom_names_accepted(self):
        # Act
        columns = SCDColumns(start_dt="dt_ini", end_dt="dt_fim", is_current="ativo", checksum="hash_n")

        # Assert
        self.assertEqual(columns.as_tuple(), ("dt_ini", "dt_fim", "ativo", "hash_n"))

    def test_blank_name_raises_value_error(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "cannot be blank"):
            SCDColumns(is_current="")

    def test_duplicate_names_raise_value_error(self):
        # Act / Assert
        with self.assertRaisesRegex(ValueError, "unique"):
            SCDColumns(is_current="dup", checksum="dup")

    def test_unsafe_identifier_raises_value_error(self):
        # Act / Assert
        for unsafe in ["is current", "is-current", "1current", "target.flag", "flag;drop"]:
            with self.subTest(name=unsafe):
                with self.assertRaisesRegex(ValueError, "Unsafe SQL identifier"):
                    SCDColumns(is_current=unsafe)

    def test_is_frozen(self):
        # Arrange
        columns = SCDColumns()

        # Act / Assert
        with self.assertRaises(Exception):
            columns.is_current = "x"


if __name__ == "__main__":
    unittest.main()
