"""Opinionated naming of the SCD control columns gandalf manages."""

from __future__ import annotations

from dataclasses import dataclass

from gandalf.scd.predicates import validate_identifier


@dataclass(frozen=True)
class SCDColumns:
    """
    Names of the four control columns gandalf owns for SCD history tables.

    The defaults are gandalf's opinionated, ``scd_``-namespaced convention. Override
    any of them to point gandalf at a table that already uses different control-column
    names (e.g. a legacy table created with ``current_flag`` / ``check_sum``)::

        SCDColumns(is_current="current_flag", checksum="check_sum")

    All four names must be valid SQL identifiers and distinct from one another.
    """

    start_dt: str = "scd_start_dt"
    end_dt: str = "scd_end_dt"
    is_current: str = "scd_is_current"
    checksum: str = "scd_checksum"

    def __post_init__(self) -> None:
        for field_name, value in (
            ("start_dt", self.start_dt),
            ("end_dt", self.end_dt),
            ("is_current", self.is_current),
            ("checksum", self.checksum),
        ):
            if not value or not str(value).strip():
                raise ValueError(f"SCDColumns.{field_name} cannot be blank")
            validate_identifier(value)
        names = self.as_tuple()
        if len(set(names)) != len(names):
            raise ValueError(f"SCDColumns names must be unique, got {names}")

    def as_tuple(self) -> tuple[str, str, str, str]:
        """Return the four control-column names in (start, end, is_current, checksum) order."""
        return (self.start_dt, self.end_dt, self.is_current, self.checksum)

    def as_set(self) -> frozenset[str]:
        """Return the four control-column names as a set (for exclusion checks)."""
        return frozenset(self.as_tuple())
