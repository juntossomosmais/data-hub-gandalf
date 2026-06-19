"""Gandalf public API."""

from gandalf.scd import SCDColumns, SCDConfig, normalize_merge_type, normalize_scd_type
from gandalf.scd.results import MergeResult

__all__ = [
    "SCDColumns",
    "SCDConfig",
    "MergeResult",
    "merge_delta_table",
    "normalize_merge_type",
    "normalize_scd_type",
    "hash_columns",
    "generate_check_sum",
    "generate_dim_sk",
    "generate_fact_sk",
    "check_duplicates",
    "column_check",
    "row_count",
    "table_check",
]


def __getattr__(name: str):
    """Lazy-load Spark/Delta-dependent API members on first access."""
    if name == "merge_delta_table":
        from gandalf.merge.merge_delta_table import merge_delta_table

        return merge_delta_table
    if name in {"hash_columns", "generate_check_sum", "generate_dim_sk", "generate_fact_sk"}:
        from gandalf.utils import df_utils

        return getattr(df_utils, name)
    if name in {"check_duplicates", "column_check", "row_count", "table_check"}:
        from gandalf.utils import df_validations

        return getattr(df_validations, name)
    raise AttributeError(f"module 'gandalf' has no attribute {name!r}")
