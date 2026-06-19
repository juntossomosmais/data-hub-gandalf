"""Canonical SCD merge type constants and legacy alias normalization."""

SCD0 = "scd0"
SCD1 = "scd1"
SCD1_DELETE = "scd1_delete"
SCD2 = "scd2"
SCD2_NO_DELETE = "scd2_no_delete"
SCD2_CDC = "scd2_cdc"
SCD3 = "scd3"
SCD4 = "scd4"
SCD6 = "scd6"

OVERWRITE = "overwrite"
OVERWRITE_PARTITION = "overwrite-partition"

SCD_TYPE_ALIASES = {
    "scd": SCD2,
    "scd_no_delete": SCD2_NO_DELETE,
    "upsert": SCD1,
    "upsert-delete": SCD1_DELETE,
}

CANONICAL_SCD_TYPES = frozenset({SCD0, SCD1, SCD1_DELETE, SCD2, SCD2_NO_DELETE, SCD2_CDC, SCD3, SCD4, SCD6})
CANONICAL_MERGE_TYPES = CANONICAL_SCD_TYPES | frozenset({OVERWRITE, OVERWRITE_PARTITION})
VALID_MERGE_TYPES = CANONICAL_MERGE_TYPES | frozenset(SCD_TYPE_ALIASES)


def normalize_scd_type(value: str) -> str:
    """Return the canonical SCD merge type for a legacy or canonical value."""
    normalized = (value or "").strip().lower()
    normalized = SCD_TYPE_ALIASES.get(normalized, normalized)
    if normalized not in CANONICAL_SCD_TYPES:
        allowed = sorted(VALID_MERGE_TYPES)
        raise ValueError(f"Invalid merge_type '{value}'. Expected one of: {allowed}")
    return normalized


def normalize_merge_type(value: str) -> str:
    """Return the canonical merge type for SCD and non-SCD values."""
    normalized = (value or "").strip().lower()
    normalized = SCD_TYPE_ALIASES.get(normalized, normalized)
    if normalized not in CANONICAL_MERGE_TYPES:
        allowed = sorted(VALID_MERGE_TYPES)
        raise ValueError(f"Invalid merge_type '{value}'. Expected one of: {allowed}")
    return normalized
