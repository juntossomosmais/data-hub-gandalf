from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp

from gandalf.merge.merge_types import overwrite, overwrite_partition
from gandalf.scd.columns import SCDColumns
from gandalf.scd.config import SCDConfig
from gandalf.scd.constants import (
    CANONICAL_SCD_TYPES,
    OVERWRITE,
    OVERWRITE_PARTITION,
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
)
from gandalf.scd.results import MergeResult
from gandalf.scd.strategies import (
    scd0_merge,
    scd1_delete_merge,
    scd1_merge,
    scd2_cdc_merge,
    scd2_merge,
    scd2_no_delete_merge,
    scd3_merge,
    scd4_merge,
    scd6_merge,
)
from gandalf.utils.df_utils import generate_check_sum, generate_dim_sk, generate_fact_sk
from gandalf.utils.df_validations import (
    check_duplicates,
    column_check,
    row_count,
    table_check,
    validate_scd_source_columns,
)

_VALID_MERGE_TYPES = VALID_MERGE_TYPES
_VALID_SK_TYPES = frozenset({"dim", "fact", ""})
_VALID_PATH_TYPES = frozenset({"storage", "table", "table_name"})
_TMZ = ZoneInfo("America/Sao_Paulo")

_STRATEGIES = {
    SCD0: scd0_merge,
    SCD1: scd1_merge,
    SCD1_DELETE: scd1_delete_merge,
    SCD2: scd2_merge,
    SCD2_NO_DELETE: scd2_no_delete_merge,
    SCD2_CDC: scd2_cdc_merge,
    SCD3: scd3_merge,
    SCD4: scd4_merge,
    SCD6: scd6_merge,
}


def _set_time_mode(spark) -> None:
    spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED")
    spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED")
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


def _convert_columns(
    spark, df: DataFrame, path: str, path_type: str, scd_skip: frozenset[str] | set[str] | None = None
) -> DataFrame:
    """Cast source columns to match target schema types (excludes SCD control columns)."""
    timestamp_fmt = "yyyy-MM-dd HH:mm:ss"
    date_fmt = "yyyy-MM-dd"

    scd_skip = set(scd_skip) if scd_skip is not None else set(SCDColumns().as_tuple())

    if path_type in ("table", "table_name"):
        schema = spark.read.table(path).dtypes
    else:
        schema = spark.read.format("delta").load(path).dtypes

    source_cols = set(df.columns)
    for col_name, col_type in schema:
        if col_name in scd_skip or col_name.startswith("sk_") or col_name not in source_cols:
            continue
        if col_type == "timestamp":
            df = df.withColumn(col_name, to_timestamp(col(col_name), timestamp_fmt))
        elif col_type == "date":
            df = df.withColumn(col_name, to_date(col(col_name), date_fmt))
        else:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df


def _read_target(spark, path: str, path_type: str) -> DataFrame:
    if path_type in ("table", "table_name"):
        return spark.read.table(path)
    return spark.read.format("delta").load(path)


def merge_delta_table(
    spark,
    df: DataFrame,
    path: str,
    ids: list,
    merge_type: str = "scd",
    *,
    sk_type: str = "",
    sk_name: str = "",
    delta_col_filter: str = "",
    merge_cond: str = "",
    ignore_update_cols: list | None = None,
    checksum_ignore_list: list | None = None,
    create_part_col: str = "",
    part_name: str = "",
    path_type: str = "table",
    scd_columns: SCDColumns | None = None,
    scd_config: SCDConfig | None = None,
) -> MergeResult | None:
    """
    Unified Delta table merge orchestrator — 10-step pipeline.

    Validates, deduplicates, generates checksum/SK, converts column types,
    executes the merge, and validates the result.

    Steps:
        1. Validate Delta table exists
        2. Filter by delta_col_filter (incremental load)
        3. Check for duplicates in the source
        4. Validate column schema
        5. Set datetime rebase mode
        6. Generate checksum (SCD types only)
        7. Generate surrogate key (if sk_type set)
        8. Convert column types to match target
        9. Execute merge
        10. Post-merge validation

    :param spark: SparkSession
    :param df: Source DataFrame
    :param path: Target Delta table path or catalog name (catalog.schema.table)
    :param ids: Column names identifying unique rows
    :param merge_type: SCD/merge behavior (legacy aliases and canonical SCD types accepted)
    :param sk_type: 'dim' (SHA-256, unique per SCD version) or 'fact' (xxhash64, deterministic)
    :param sk_name: SK column name (convention: sk_{entity})
    :param delta_col_filter: Column to filter source by max value in target (incremental)
    :param merge_cond: Extra raw-SQL merge/partition condition appended to the generated
        predicate. TRUSTED INPUT ONLY — it is interpolated verbatim into the Delta MERGE
        condition / DataFrame filter; never pass untrusted or user-derived strings.
    :param ignore_update_cols: Columns to exclude from the upsert update set
    :param checksum_ignore_list: Columns excluded from checksum (e.g. ["created_at", "updated_at"])
    :param create_part_col: Source column used to derive a partition column
    :param part_name: Partition column name to create on target
    :param path_type: 'storage' (ADLS path) or 'table' (Unity Catalog name).
    :param scd_columns: Override the names of the four SCD control columns gandalf manages
        (defaults to ``SCDColumns()``: scd_start_dt / scd_end_dt / scd_is_current / scd_checksum).
        Use it to point gandalf at a table that already uses different control-column names.
        Mutually exclusive with ``scd_config`` (which carries its own ``columns``).
    :param scd_config: Typed configuration for the advanced SCD types (scd0/scd3/scd4/scd6),
        observability (``return_metrics``), and custom control-column names. These types are
        only reachable through ``scd_config``.
    :return: MergeResult when ``scd_config.return_metrics`` is set, otherwise None

    Examples::
    # Silver SCD2 table (full snapshot source)
    merge_delta_table(spark, df, "main.silver.client", ["id_client"],
                      merge_type="scd", checksum_ignore_list=["created_at", "updated_at"])

    # Silver SCD2 table (incremental source — no delete detection)
    merge_delta_table(spark, df, "main.silver.client", ["id_client"],
                      merge_type="scd_no_delete", delta_col_filter="updated_at",
                      checksum_ignore_list=["created_at", "updated_at"])

    # Gold dimension table
    merge_delta_table(spark, df, "main.gold.dim_client", ["id_client"],
                      merge_type="scd_no_delete", sk_type="dim", sk_name="sk_client",
                      checksum_ignore_list=["created_at", "updated_at"])

    # Gold fact table
    merge_delta_table(spark, df, "main.gold.fact_orders", ["id_order"],
                      merge_type="upsert", sk_type="fact", sk_name="sk_order")

    # Existing table with legacy control-column names
    merge_delta_table(spark, df, "main.silver.client", ["id_client"],
                      merge_type="scd",
                      scd_columns=SCDColumns(is_current="current_flag", checksum="check_sum"))
    """
    if ignore_update_cols is None:
        ignore_update_cols = []
    if checksum_ignore_list is None:
        checksum_ignore_list = []

    if scd_columns is not None and scd_config is not None:
        raise ValueError("Pass scd_columns OR scd_config (which carries its own columns), not both")

    tmz = _TMZ
    log_item = {
        "path": path,
        "ids": ids,
        "merge_type": merge_type,
        "sk_type": sk_type,
        "sk_name": sk_name,
        "path_type": path_type,
    }
    start_time = datetime.now(tmz).timestamp()

    # — Parameter validation —
    canonical_merge_type = normalize_merge_type(merge_type)
    if scd_config is not None:
        canonical_merge_type = scd_config.scd_type
        ids = scd_config.ids
        columns = scd_config.columns
    elif canonical_merge_type in CANONICAL_SCD_TYPES:
        columns = scd_columns or SCDColumns()
        scd_config = SCDConfig.from_legacy(
            merge_type=merge_type,
            ids=ids,
            checksum_ignore_list=checksum_ignore_list,
            columns=columns,
        )
    else:
        columns = scd_columns or SCDColumns()

    log_item["merge_type"] = canonical_merge_type
    log_item["ids"] = ids

    if sk_type not in _VALID_SK_TYPES:
        raise ValueError(f"Invalid sk_type '{sk_type}'. Expected one of: {sorted(_VALID_SK_TYPES)}")

    if sk_type and not sk_name:
        raise ValueError("sk_name is required when sk_type is set")

    if sk_name and not sk_type:
        raise ValueError("sk_type is required when sk_name is set")

    if canonical_merge_type in (SCD2, SCD2_NO_DELETE) and ignore_update_cols:
        raise ValueError("ignore_update_cols is not valid for SCD2 merge types")

    if canonical_merge_type == SCD2 and delta_col_filter:
        raise ValueError("delta_col_filter cannot be used with merge_type='scd'/'scd2' (full snapshot mode)")

    if path_type not in _VALID_PATH_TYPES:
        raise ValueError(f"Invalid path_type '{path_type}'. Expected one of: {sorted(_VALID_PATH_TYPES)}")

    try:
        # Step 1 — Validate Delta table exists
        table_check(spark, path, path_type)
        print(f"[1] Target table found — {datetime.now(tmz).strftime('%H:%M:%S')}")

        # Step 2 — Filter by delta column (incremental load)
        if delta_col_filter:
            target_df = _read_target(spark, path, path_type)
            max_val = target_df.select(delta_col_filter).agg({delta_col_filter: "max"}).collect()[0][0]
            if max_val is not None:
                df = df.filter(col(delta_col_filter) > max_val)
            now = datetime.now(tmz).strftime("%H:%M:%S")
            print(f"[2] Filtered by max({delta_col_filter}) = {max_val} — {now}")

        # Step 3 — Duplicate check
        check_duplicates(df, ids, False)
        print(f"[3] No duplicates found — {datetime.now(tmz).strftime('%H:%M:%S')}")

        # Step 4 — Column schema check
        extra_target_exclude = set()
        if scd_config and scd_config.previous_columns:
            extra_target_exclude = set(scd_config.previous_columns.values())
        column_check(
            spark,
            df,
            path,
            path_type,
            sk_name=sk_name,
            create_part_col=create_part_col,
            part_name=part_name,
            extra_target_exclude=extra_target_exclude or None,
            scd_control_cols=columns.as_set(),
        )
        print(f"[4] Columns match — {datetime.now(tmz).strftime('%H:%M:%S')}")

        # Step 5 — Set datetime rebase mode
        _set_time_mode(spark)
        print(f"[5] datetime rebase mode set — {datetime.now(tmz).strftime('%H:%M:%S')}")

        # Step 6 — Generate checksum (history/change-detection SCD types only)
        if canonical_merge_type in (SCD2, SCD2_NO_DELETE, SCD2_CDC, SCD4, SCD6):
            tracked_columns = scd_config.tracked_columns if scd_config else None
            checksum_ignored = scd_config.checksum_ignore_columns if scd_config else checksum_ignore_list
            df = generate_check_sum(
                df,
                checksum_ignored,
                tracked_columns,
                checksum_col=columns.checksum,
                scd_control_cols=columns.as_set(),
            )
            print(f"[6] Checksum generated — {datetime.now(tmz).strftime('%H:%M:%S')}")

        # Step 7 — Generate surrogate key
        if sk_name:
            if sk_type == "dim":
                df = generate_dim_sk(df, ids, sk_name, checksum_col=columns.checksum)
                print(f"[7] Dim SK '{sk_name}' generated — {datetime.now(tmz).strftime('%H:%M:%S')}")
            elif sk_type == "fact":
                df = generate_fact_sk(df, ids, sk_name)
                print(f"[7] Fact SK '{sk_name}' generated — {datetime.now(tmz).strftime('%H:%M:%S')}")

        # Step 8 — Convert column types to match target
        df = _convert_columns(spark, df, path, path_type, scd_skip=columns.as_set())
        print(f"[8] Columns converted — {datetime.now(tmz).strftime('%H:%M:%S')}")

        # Step 9 — Row count (before merge, for post-validation)
        df_count = df.count()

        # Step 9 — Execute merge
        if canonical_merge_type in CANONICAL_SCD_TYPES:
            if scd_config:
                validate_scd_source_columns(df, scd_config)
            effective_ids = [sk_name] if sk_type == "fact" and canonical_merge_type in {SCD1, SCD1_DELETE} else ids
            _STRATEGIES[canonical_merge_type](
                spark,
                df,
                effective_ids,
                path,
                path_type,
                config=scd_config,
                merge_cond=merge_cond,
                ignore_update_cols=ignore_update_cols,
                create_part_col=create_part_col,
                part_name=part_name,
            )

        elif canonical_merge_type == OVERWRITE_PARTITION:
            overwrite_partition(spark, df, path, merge_cond, path_type)

        elif canonical_merge_type == OVERWRITE:
            overwrite(spark, df, path, path_type)

        now = datetime.now(tmz).strftime("%H:%M:%S")
        print(f"[9] Merge '{canonical_merge_type}' completed at {path} — {now}")

        # Step 10 — Post-merge validation
        target_df = _read_target(spark, path, path_type)

        if merge_cond and canonical_merge_type == OVERWRITE_PARTITION:
            target_df = target_df.filter(merge_cond)

        # Strict source==current row-count check applies only to full-snapshot SCD2 ('scd').
        # It is intentionally skipped for scd_no_delete (incremental source — count differs)
        # and whenever merge_cond is set (partial snapshot — the guard would false-positive).
        if canonical_merge_type == SCD2 and not merge_cond:
            row_count(df_count, target_df, current_flag_col=columns.is_current)

        if canonical_merge_type in (SCD2, SCD2_NO_DELETE, SCD6):
            check_duplicates(target_df, ids, True, current_flag_col=columns.is_current)
            check_duplicates(target_df, ids + [columns.end_dt], False)
        elif canonical_merge_type in (
            SCD0,
            SCD1,
            SCD1_DELETE,
            SCD3,
            SCD4,
            OVERWRITE_PARTITION,
            OVERWRITE,
        ):
            check_duplicates(target_df, ids, False)

        print(f"[10] Post-merge validation passed — {datetime.now(tmz).strftime('%H:%M:%S')}")

        elapsed = round(datetime.now(tmz).timestamp() - start_time, 1)
        print(f"merge_delta_table completed in {elapsed}s — {log_item}")
        if scd_config and scd_config.return_metrics:
            return MergeResult(path=path, merge_type=canonical_merge_type, source_rows=df_count)
        return None

    except Exception as e:
        elapsed = round(datetime.now(tmz).timestamp() - start_time, 1)
        print(f"merge_delta_table FAILED after {elapsed}s — {log_item} — Error: {e}")
        raise
