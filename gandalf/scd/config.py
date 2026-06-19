"""Configuration primitives for Gandalf SCD strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from gandalf.scd.columns import SCDColumns
from gandalf.scd.constants import SCD0, SCD2, SCD2_CDC, SCD3, SCD4, SCD6, normalize_scd_type


@dataclass(frozen=True)
class SCDConfig:
    """Typed SCD strategy configuration with eager validation."""

    scd_type: str
    ids: list[str]
    tracked_columns: list[str] | None = None
    checksum_ignore_columns: list[str] = field(default_factory=list)
    protected_columns: list[str] = field(default_factory=list)
    previous_columns: dict[str, str] = field(default_factory=dict)
    history_path: str | None = None
    history_path_type: str | None = None
    columns: SCDColumns = field(default_factory=SCDColumns)
    delete_mode: Literal["ignore", "soft", "hard"] = "ignore"
    cdc_timestamp_col: str = "timestamp_dms"
    return_metrics: bool = False

    @property
    def effective_from_col(self) -> str:
        return self.columns.start_dt

    @property
    def effective_to_col(self) -> str:
        return self.columns.end_dt

    @property
    def current_flag_col(self) -> str:
        return self.columns.is_current

    @property
    def checksum_col(self) -> str:
        return self.columns.checksum

    def __post_init__(self) -> None:
        object.__setattr__(self, "scd_type", normalize_scd_type(self.scd_type))
        object.__setattr__(self, "ids", list(self.ids or []))
        object.__setattr__(self, "checksum_ignore_columns", list(self.checksum_ignore_columns or []))
        object.__setattr__(self, "protected_columns", list(self.protected_columns or []))
        object.__setattr__(self, "previous_columns", dict(self.previous_columns or {}))
        if self.tracked_columns is not None:
            object.__setattr__(self, "tracked_columns", list(self.tracked_columns))
        if self.columns is None:
            object.__setattr__(self, "columns", SCDColumns())
        elif not isinstance(self.columns, SCDColumns):
            raise TypeError("SCDConfig.columns must be an SCDColumns instance")
        self.validate()

    @classmethod
    def from_legacy(
        cls,
        merge_type: str,
        ids: list[str],
        checksum_ignore_list: list[str] | None = None,
        protected_columns: list[str] | None = None,
        previous_columns: dict[str, str] | None = None,
        history_path: str | None = None,
        history_path_type: str | None = None,
        columns: SCDColumns | None = None,
        **overrides,
    ) -> SCDConfig:
        """Build an SCDConfig from merge_delta_table flat parameters."""
        values: dict = {
            "scd_type": merge_type,
            "ids": ids,
            "checksum_ignore_columns": checksum_ignore_list or [],
        }
        if protected_columns:
            values["protected_columns"] = protected_columns
        if previous_columns:
            values["previous_columns"] = previous_columns
        if history_path:
            values["history_path"] = history_path
        if history_path_type:
            values["history_path_type"] = history_path_type
        if columns is not None:
            values["columns"] = columns
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)

    def validate(self) -> None:
        if not self.ids:
            raise ValueError("SCDConfig ids must be a non-empty list")
        self._validate_no_blank_columns("ids", self.ids)
        self._validate_no_blank_columns("checksum_ignore_columns", self.checksum_ignore_columns)
        self._validate_no_blank_columns("protected_columns", self.protected_columns)
        if self.tracked_columns is not None:
            self._validate_no_blank_columns("tracked_columns", self.tracked_columns)
        if self.delete_mode not in {"ignore", "soft", "hard"}:
            raise ValueError("delete_mode must be one of: ignore, soft, hard")

        if self.scd_type == SCD4 and not self.history_path:
            raise ValueError("SCD4 requires history_path")
        if self.scd_type in {SCD3, SCD6} and not self.previous_columns:
            raise ValueError(f"{self.scd_type} requires previous_columns")
        if self.scd_type == SCD0 and not (self.protected_columns or self.tracked_columns):
            raise ValueError("SCD0 requires protected_columns or tracked_columns")
        if self.scd_type in {SCD2, SCD6}:
            self._validate_no_blank_columns(
                "SCD2 control columns",
                [
                    self.effective_from_col,
                    self.effective_to_col,
                    self.current_flag_col,
                    self.checksum_col,
                ],
            )
        if self.scd_type == SCD2_CDC and not self.cdc_timestamp_col:
            raise ValueError("SCD2 CDC requires cdc_timestamp_col")

        control_names = set(self.columns.as_tuple())
        id_collisions = control_names & set(self.ids)
        if id_collisions:
            raise ValueError(f"SCD control columns collide with ids: {sorted(id_collisions)}")
        previous_names = set(self.previous_columns) | set(self.previous_columns.values())
        previous_collisions = control_names & previous_names
        if previous_collisions:
            raise ValueError(f"SCD control columns collide with previous_columns: {sorted(previous_collisions)}")

        self._validate_previous_column_collisions()

    @staticmethod
    def _validate_no_blank_columns(context: str, columns: list[str]) -> None:
        if any(not c or not str(c).strip() for c in columns):
            raise ValueError(f"{context} cannot contain blank column names")

    def _validate_previous_column_collisions(self) -> None:
        id_set = set(self.ids)
        previous_keys = set(self.previous_columns)
        previous_values = set(self.previous_columns.values())
        if previous_keys & id_set or previous_values & id_set:
            raise ValueError("previous_columns cannot collide with ids")
