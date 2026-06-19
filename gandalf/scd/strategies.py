"""
SCD strategy functions used by merge_delta_table.

The implementations intentionally delegate existing legacy behavior where possible
and add focused first-pass strategies for SCD0/3/4/6.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp

from gandalf.merge.merge_types import _get_delta_table, _write_df, scd2_batch, upsert_merge
from gandalf.scd.config import SCDConfig
from gandalf.scd.predicates import append_extra_condition, build_key_condition, quote_identifier
from gandalf.utils.df_validations import (
    check_duplicates,
    validate_no_duplicate_current,
    validate_scd_target_columns,
)


class SCDValidationError(ValueError):
    """Raised when source/target data violates an SCD contract."""


def scd0_merge(
    spark,
    df: DataFrame,
    ids: list[str],
    path: str,
    path_type: str = "storage",
    config: SCDConfig | None = None,
    merge_cond: str = "",
    **_,
) -> None:
    """SCD0: insert new keys and reject protected-column changes for existing keys."""
    if config is None:
        raise ValueError("scd0_merge requires SCDConfig with protected_columns or tracked_columns")
    protected_columns = config.protected_columns or config.tracked_columns or []
    dt_target = _get_delta_table(spark, path, path_type)
    target_df = dt_target.toDF()

    for protected_col in protected_columns:
        changed = (
            df.alias("source")
            .join(target_df.alias("target"), ids, "inner")
            .where(~(col(f"source.{protected_col}").eqNullSafe(col(f"target.{protected_col}"))))
            .limit(1)
            .count()
        )
        if changed:
            raise SCDValidationError(f"SCD0 protected column changed: {protected_col}")

    cond = append_extra_condition(build_key_condition("target", "source", ids), merge_cond)
    dt_target.alias("target").merge(df.alias("source"), cond).whenNotMatchedInsertAll().execute()


def scd1_merge(
    spark,
    df: DataFrame,
    ids: list[str],
    path: str,
    path_type: str = "storage",
    merge_cond: str = "",
    ignore_update_cols: list[str] | None = None,
    has_delete: bool = False,
    **_,
) -> None:
    """SCD1: overwrite current target rows, preserving legacy upsert behavior."""
    upsert_merge(
        spark,
        df,
        ids,
        path,
        path_type,
        merge_cond=merge_cond,
        ignore_update_cols=ignore_update_cols or [],
        has_delete=has_delete,
    )


def scd1_delete_merge(*args, **kwargs) -> None:
    """SCD1 with hard delete for absent target rows."""
    kwargs["has_delete"] = True
    scd1_merge(*args, **kwargs)


def scd2_merge(
    spark,
    df: DataFrame,
    ids: list[str],
    path: str,
    path_type: str = "storage",
    merge_cond: str = "",
    create_part_col: str = "",
    part_name: str = "",
    ignore_delete: bool = False,
    config: SCDConfig | None = None,
    **_,
) -> None:
    """SCD2 batch merge wrapper."""
    scd2_batch(
        spark,
        df,
        ids,
        path,
        path_type,
        ignore_delete=ignore_delete,
        merge_cond=merge_cond,
        create_part_col=create_part_col,
        part_name=part_name,
        columns=config.columns if config is not None else None,
    )


def scd2_no_delete_merge(*args, **kwargs) -> None:
    """SCD2 incremental/no-delete wrapper."""
    kwargs["ignore_delete"] = True
    scd2_merge(*args, **kwargs)


def scd2_cdc_merge(*_, **__) -> None:
    """Explicitly handle accepted CDC type until late-event-safe semantics are implemented."""
    raise NotImplementedError(
        "merge_type='scd2_cdc' is recognized but requires event-order/late-event semantics; "
        "it is intentionally rejected instead of silently no-oping."
    )


def scd3_merge(
    spark,
    df: DataFrame,
    ids: list[str],
    path: str,
    path_type: str = "storage",
    config: SCDConfig | None = None,
    merge_cond: str = "",
    ignore_update_cols: list[str] | None = None,
    **_,
) -> None:
    """SCD3: update current values and shift configured columns into previous columns."""
    if config is None:
        raise ValueError("scd3_merge requires SCDConfig with previous_columns")
    dt_target = _get_delta_table(spark, path, path_type)
    validate_scd_target_columns(dt_target.toDF(), config)

    cond = append_extra_condition(build_key_condition("target", "source", ids), merge_cond)
    ignored = set(ignore_update_cols or []) | set(ids) | set(config.previous_columns.values())
    schema_to_update: dict[str, Column | str] = {c: col(f"source.{c}") for c in df.columns if c not in ignored}
    for current_col, previous_col in config.previous_columns.items():
        schema_to_update[previous_col] = col(f"target.{current_col}")
    insert_values: dict[str, Column | str] = {c: col(f"source.{c}") for c in df.columns}
    for previous_col in config.previous_columns.values():
        if previous_col not in insert_values:
            insert_values[previous_col] = lit(None)

    # Detect change in ANY business column — not just previous_columns.keys().
    # If only a non-tracked column changes (e.g. email when only address is in
    # previous_columns), the row must still be updated with the new value.
    all_trackable = sorted(
        c for c in df.columns if c not in set(ids) and c not in set(config.previous_columns.values())
    )
    condition = " OR ".join(
        f"NOT (source.{quote_identifier(c)} <=> target.{quote_identifier(c)})" for c in all_trackable
    )
    dt_target.alias("target").merge(df.alias("source"), cond).whenMatchedUpdate(
        condition=condition,
        set=schema_to_update,
    ).whenNotMatchedInsert(values=insert_values).execute()


def scd4_merge(
    spark,
    df: DataFrame,
    ids: list[str],
    path: str,
    path_type: str = "storage",
    config: SCDConfig | None = None,
    merge_cond: str = "",
    ignore_update_cols: list[str] | None = None,
    **_,
) -> None:
    """
    SCD4: Type 1 current table plus append-only history for changed/new rows.

    df is expected to already carry the checksum column (generated by merge_delta_table step 6).
    History is only appended when a key is new or its checksum differs from the current target.
    Falls back to full append when target has no checksum column (backward-compatible).
    """
    if config is None:
        raise ValueError("scd4_merge requires SCDConfig with history_path")
    if config.history_path is None:
        raise ValueError("scd4_merge requires SCDConfig.history_path to be set")

    history_path = config.history_path
    history_path_type = config.history_path_type or path_type
    checksum_col = config.checksum_col

    if checksum_col not in df.columns:
        raise ValueError(
            f"scd4_merge: source DataFrame is missing checksum column '{checksum_col}'. "
            "merge_delta_table generates this at step 6 — ensure merge_type='scd4' is "
            "dispatched through merge_delta_table, not called directly."
        )

    # df already has checksum — just stamp effective timestamps for the history record
    history_candidate = df.withColumn(config.effective_from_col, lit(datetime.now())).withColumn(
        config.effective_to_col, to_timestamp(lit("9999-12-31 00:00:00"))
    )

    dt_target = _get_delta_table(spark, path, path_type)
    target_df = dt_target.toDF()

    if target_df.limit(1).count() == 0:
        # Initial load — every source row is new
        _write_df(history_candidate, history_path, history_path_type, mode="append")
    elif checksum_col in target_df.columns:
        _TGT_CS = "_scd4_target_cs"
        target_checksums = target_df.select(*ids, checksum_col).withColumnRenamed(checksum_col, _TGT_CS)
        changed_or_new = (
            history_candidate.join(target_checksums, ids, "left")
            .where(col(_TGT_CS).isNull() | ~col(checksum_col).eqNullSafe(col(_TGT_CS)))
            .drop(_TGT_CS)
        )
        _write_df(changed_or_new, history_path, history_path_type, mode="append")
    else:
        # Target has no checksum column — safe fallback to full append
        _write_df(history_candidate, history_path, history_path_type, mode="append")

    scd1_merge(
        spark,
        df,
        ids,
        path,
        path_type,
        merge_cond=merge_cond,
        ignore_update_cols=ignore_update_cols,
    )


def scd6_merge(
    spark,
    df: DataFrame,
    ids: list[str],
    path: str,
    path_type: str = "storage",
    config: SCDConfig | None = None,
    merge_cond: str = "",
    create_part_col: str = "",
    part_name: str = "",
    **_,
) -> None:
    """SCD6 first pass: Type 2 versioning with previous columns populated from current target."""
    if config is None:
        raise ValueError("scd6_merge requires SCDConfig with previous_columns")
    dt_target = _get_delta_table(spark, path, path_type)
    target_df = dt_target.toDF()
    validate_scd_target_columns(target_df, config)

    if target_df.limit(1).count() == 0:
        seeded = df
        for previous_col in config.previous_columns.values():
            if previous_col not in seeded.columns:
                seeded = seeded.withColumn(previous_col, lit(None))
        scd2_batch(
            spark, seeded, ids, path, path_type, ignore_delete=True, merge_cond=merge_cond, columns=config.columns
        )
        return

    joined = df.alias("source").join(
        target_df.filter(col(config.current_flag_col) == 1).alias("target"),
        ids,
        "left",
    )
    select_exprs = [col(f"source.{c}").alias(c) for c in df.columns]
    for current_col, previous_col in config.previous_columns.items():
        select_exprs.append(col(f"target.{current_col}").alias(previous_col))
    enriched = joined.select(*select_exprs)
    # A duplicate current row in the target would fan the left join out into duplicate keys
    # here, which scd2_batch's MERGE rejects as multiple-source-rows-matched. Fail early and
    # clearly instead. (Healthy targets have unique current rows, so this is a no-op.)
    check_duplicates(enriched, ids, False)
    scd2_batch(
        spark,
        enriched,
        ids,
        path,
        path_type,
        ignore_delete=True,
        merge_cond=merge_cond,
        create_part_col=create_part_col,
        part_name=part_name,
        columns=config.columns,
    )
    validate_no_duplicate_current(_get_delta_table(spark, path, path_type).toDF(), ids, config.current_flag_col)
