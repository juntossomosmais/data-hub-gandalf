"""Public SCD configuration and strategy helpers."""

from gandalf.scd.columns import SCDColumns
from gandalf.scd.config import SCDConfig
from gandalf.scd.constants import (
    CANONICAL_MERGE_TYPES,
    CANONICAL_SCD_TYPES,
    SCD0,
    SCD1,
    SCD1_DELETE,
    SCD2,
    SCD2_CDC,
    SCD2_NO_DELETE,
    SCD3,
    SCD4,
    SCD6,
    VALID_MERGE_TYPES,
    normalize_merge_type,
    normalize_scd_type,
)

__all__ = [
    "CANONICAL_MERGE_TYPES",
    "CANONICAL_SCD_TYPES",
    "SCD0",
    "SCD1",
    "SCD1_DELETE",
    "SCD2",
    "SCD2_CDC",
    "SCD2_NO_DELETE",
    "SCD3",
    "SCD4",
    "SCD6",
    "SCDColumns",
    "SCDConfig",
    "VALID_MERGE_TYPES",
    "normalize_merge_type",
    "normalize_scd_type",
]
