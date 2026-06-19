import re
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit, to_timestamp, trunc

from gandalf.exceptions import InvalidColumnsError
from gandalf.scd.columns import SCDColumns
from gandalf.scd.predicates import append_extra_condition, build_key_condition, quote_identifier


def _get_delta_table(spark, path: str, path_type: str) -> DeltaTable:
    """Resolve DeltaTable from path or catalog name."""
    if path_type in ("table", "table_name"):
        return DeltaTable.forName(spark, path)
    return DeltaTable.forPath(spark, path)


def _write_df(df: DataFrame, path: str, path_type: str, mode: str = "overwrite") -> None:
    """Write DataFrame to Delta — handles storage path or catalog table."""
    if path_type in ("table", "table_name"):
        df.write.format("delta").mode(mode).saveAsTable(path)
    else:
        df.write.format("delta").mode(mode).save(path)


def scd2_batch(
    spark,
    df: DataFrame,
    ids: list,
    path: str,
    path_type: str = "storage",
    ignore_delete: bool = False,
    merge_cond: str = "",
    create_part_col: str = "",
    part_name: str = "",
    columns: SCDColumns | None = None,
) -> None:
    """
    Execute a SCD Type 2 batch merge.

    On empty target: initial load with SCD control columns.
    On existing target: detect inserts, updates, and optionally deletes. The target
    must already carry the four control columns under the configured ``columns`` names.

    :param spark: SparkSession
    :param df: Source DataFrame (with the checksum column)
    :param ids: Business key column names
    :param path: Target Delta table path or catalog name
    :param path_type: 'storage' or 'table' (also accepts 'table_name')
    :param ignore_delete: If True, rows absent from source are not marked deleted (scd_no_delete)
    :param merge_cond: Extra raw-SQL merge condition. TRUSTED INPUT ONLY — interpolated
        verbatim into the Delta MERGE condition; never pass untrusted strings.
    :param create_part_col: Source column used to derive a partition column
    :param part_name: Name of the partition column to create
    :param columns: SCD control-column names (default: ``SCDColumns()``)
    :raises InvalidColumnsError: when an existing target lacks the configured control columns

    Example::
    scd2_batch(spark, df, ["id_client"], "main.silver.client", "table")
    scd2_batch(..., ignore_delete=True)
    """
    cols = columns or SCDColumns()
    is_current, start_dt, end_dt, checksum = cols.is_current, cols.start_dt, cols.end_dt, cols.checksum
    q_is_current, q_checksum = quote_identifier(is_current), quote_identifier(checksum)

    merge_key_cols = {"merge_key"} | {f"merge_key{i + 1}" for i in range(len(ids))}
    staging_collisions = merge_key_cols & set(df.columns)
    if staging_collisions:
        raise InvalidColumnsError(
            f"\nSource columns collide with internal SCD2 staging columns {sorted(staging_collisions)}; "
            "rename them before merging."
        )

    dt_target = _get_delta_table(spark, path, path_type)
    target_df = dt_target.toDF()
    # cts is the *client/driver* clock (intentionally not current_timestamp()): all staged
    # DataFrames below must share one fixed instant for the SCD version bounds.
    cts = datetime.now()
    cts_minus_one = cts - timedelta(milliseconds=1)

    set_dict: dict[str, Column | str] = {is_current: lit(0), end_dt: lit(cts_minus_one)}

    if target_df.limit(1).count() == 0:
        photo = (
            df.withColumn(is_current, lit(1))
            .withColumn(start_dt, lit(cts))
            .withColumn(end_dt, to_timestamp(lit("9999-12-31 00:00:00")))
        )
        if create_part_col:
            photo = photo.withColumn(part_name, trunc(col(create_part_col), "mm"))
        _write_df(photo, path, path_type, mode="overwrite")
        return

    missing = [c for c in (is_current, start_dt, end_dt, checksum) if c not in target_df.columns]
    if missing:
        raise InvalidColumnsError(
            f"\nTarget table is missing SCD2 control columns {sorted(missing)}. "
            f"gandalf manages {[start_dt, end_dt, is_current, checksum]}; pass "
            "scd_columns=SCDColumns(...) to match the names your table already uses."
        )

    target_dtypes = dict(target_df.dtypes)
    if target_dtypes.get(checksum) != "string":
        raise InvalidColumnsError(
            f"\nSCD2 checksum column '{checksum}' must be StringType, found '{target_dtypes.get(checksum)}'. "
            "The delete sentinel is written as a string."
        )
    if target_dtypes.get(is_current) not in {"tinyint", "smallint", "int", "bigint"}:
        raise InvalidColumnsError(
            f"\nSCD2 current-flag column '{is_current}' must be an integer (1/0) type, "
            f"found '{target_dtypes.get(is_current)}'."
        )

    join_list = [col("source." + id_) == col("target." + id_) for id_ in ids]

    new_and_inactive_values = (
        df.withColumn(is_current, lit(1))
        .withColumn(start_dt, lit(cts))
        .withColumn(end_dt, to_timestamp(lit("9999-12-31 00:00:00")))
    )

    if len(ids) > 1:
        for idx, id_ in enumerate(ids):
            new_and_inactive_values = new_and_inactive_values.withColumn(f"merge_key{idx + 1}", col(id_))
    else:
        new_and_inactive_values = new_and_inactive_values.withColumn("merge_key", col(ids[0]))

    new_values_to_update = (
        df.alias("source")
        .join(target_df.alias("target"), join_list)
        .where(f"target.{q_is_current} = 1 AND source.{q_checksum} <> target.{q_checksum}")
        .selectExpr("source.*")
        .withColumn(is_current, lit(1))
        .withColumn(start_dt, lit(cts))
        .withColumn(end_dt, to_timestamp(lit("9999-12-31 00:00:00")))
    )

    if len(ids) > 1:
        for idx, id_ in enumerate(ids):
            new_values_to_update = new_values_to_update.withColumn(f"merge_key{idx + 1}", lit(None))
    else:
        new_values_to_update = new_values_to_update.withColumn("merge_key", lit(None))

    if ignore_delete:
        staged_updates = new_values_to_update.union(new_and_inactive_values.select(new_values_to_update.columns))
    else:
        values_to_delete = (
            target_df.alias("target")
            .where(f"{q_is_current} = 1")
            .join(df.alias("source"), join_list, "leftanti")
            .selectExpr("target.*")
            .withColumn(is_current, lit(0))
            .withColumn(start_dt, to_timestamp(lit(None)))
            .withColumn(end_dt, lit(cts))
            .withColumn(checksum, lit("deleted"))
        )

        if len(ids) > 1:
            for idx, id_ in enumerate(ids):
                values_to_delete = values_to_delete.withColumn(f"merge_key{idx + 1}", col(id_))
        else:
            values_to_delete = values_to_delete.withColumn("merge_key", col(ids[0]))

        staged_updates = new_values_to_update.union(new_and_inactive_values.select(new_values_to_update.columns)).union(
            values_to_delete.select(new_values_to_update.columns)
        )

    if create_part_col:
        staged_updates = staged_updates.withColumn(part_name, trunc(col(create_part_col), "mm"))
        if create_part_col == end_dt:
            set_dict[part_name] = trunc(lit(cts_minus_one), "mm")

    unwanted_cols = {end_dt, "merge_key"} | {f"merge_key{i + 1}" for i in range(len(ids))}
    schema_list = [c for c in staged_updates.columns if c not in unwanted_cols]
    # Qualify insert columns with the source alias. A bare col(c) is ambiguous in the MERGE
    # (both `target` and `staged_updates` expose the same names); Spark 4.0's analyzer rejects
    # it with AMBIGUOUS_REFERENCE, while older analyzers silently resolved it to the source.
    schema_to_insert: dict[str, Column | str] = {c: col(f"staged_updates.{c}") for c in schema_list}
    schema_to_insert[end_dt] = to_timestamp(lit("9999-12-31 00:00:00"))

    if len(ids) > 1:
        id_cond = " AND ".join(
            f"target.{quote_identifier(id_)} = staged_updates.{quote_identifier(f'merge_key{idx + 1}')}"
            for idx, id_ in enumerate(ids)
        )
        id_cond += f" AND target.{q_is_current} = 1"
    else:
        id_cond = (
            f"target.{quote_identifier(ids[0])} = staged_updates.{quote_identifier('merge_key')} "
            f"AND target.{q_is_current} = 1"
        )

    cond = append_extra_condition(id_cond, merge_cond)

    (
        dt_target.alias("target")
        .merge(staged_updates.alias("staged_updates"), cond)
        .whenMatchedUpdate(
            condition=f"target.{q_is_current} = 1 AND staged_updates.{q_checksum} <> target.{q_checksum}",
            set=set_dict,
        )
        .whenNotMatchedInsert(values=schema_to_insert)
        .execute()
    )


def upsert_merge(
    spark,
    df: DataFrame,
    ids: list,
    path: str,
    path_type: str = "storage",
    merge_cond: str = "",
    ignore_update_cols: list | None = None,
    has_delete: bool = False,
) -> None:
    """
    Merge source rows into the target: update matched rows, insert unmatched rows.

    :param spark: SparkSession
    :param df: Source DataFrame
    :param ids: Column names used as join keys
    :param path: Target Delta table path or catalog name
    :param path_type: 'storage' or 'table' (also accepts 'table_name')
    :param merge_cond: Extra raw-SQL merge condition. TRUSTED INPUT ONLY — interpolated
        verbatim into the Delta MERGE condition / filter; never pass untrusted strings.
    :param ignore_update_cols: Columns to exclude from the update set
    :param has_delete: If True, rows absent from source are deleted from target

    Example::
    upsert_merge(spark, df, ["id_product"], "main.silver.product", "table")
    """
    if ignore_update_cols is None:
        ignore_update_cols = []

    dt_target = _get_delta_table(spark, path, path_type)
    schema_list = [c for c in df.columns if c not in ignore_update_cols]
    schema_to_update: dict[str, Column | str] = {c: col(f"source.{c}") for c in schema_list}

    id_cond = build_key_condition("target", "source", ids)

    cond = append_extra_condition(id_cond, merge_cond)

    if not has_delete:
        (
            dt_target.alias("target")
            .merge(df.alias("source"), cond)
            .whenMatchedUpdate(set=schema_to_update)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df_delete = dt_target.toDF().alias("target").join(df, ids, "left_anti")
        if merge_cond:
            df_delete = df_delete.filter(merge_cond)
        df_delete = df_delete.withColumn("deleted", lit(1))
        df_upsert = df.withColumn("deleted", lit(0))
        df_merged = df_upsert.unionByName(df_delete)

        (
            dt_target.alias("target")
            .merge(df_merged.alias("source"), cond)
            .whenMatchedDelete(condition="source.deleted = 1")
            .whenMatchedUpdate(set=schema_to_update)
            .whenNotMatchedInsertAll()
            .execute()
        )


def overwrite_partition(
    spark,
    df: DataFrame,
    path: str,
    merge_cond: str,
    path_type: str = "storage",
) -> None:
    """
    Overwrite specific partitions using replaceWhere.

    All partition columns must be present in merge_cond to avoid data loss. ``merge_cond``
    must be a conjunction of partition constraints — ``OR`` / ``IS NOT NULL`` are rejected
    because they can widen the ``replaceWhere`` predicate into replacing unintended partitions.

    :param spark: SparkSession
    :param df: Source DataFrame
    :param path: Target Delta table path or catalog name
    :param merge_cond: Partition filter string (e.g. "year_month = '2024-01'")
    :param path_type: 'storage' or 'table' (also accepts 'table_name')

    Example::
    overwrite_partition(spark, df, "table", "year_month = '2024-01'", "table")
    """
    if re.search(r"\bOR\b", merge_cond, flags=re.IGNORECASE) or re.search(
        r"\bIS\s+NOT\s+NULL\b", merge_cond, flags=re.IGNORECASE
    ):
        raise ValueError(
            f"overwrite-partition merge_cond must be a conjunction of partition constraints; "
            f"'OR' / 'IS NOT NULL' risk replacing unintended partitions (replaceWhere data loss): {merge_cond!r}"
        )

    if path_type in ("table", "table_name"):
        detail_expr = f"describe detail {path}"
    else:
        detail_expr = f"describe detail delta.`{path}`"

    existing_partitions = spark.sql(detail_expr).select("partitionColumns").collect()[0]["partitionColumns"]

    def find_partition(w):
        return re.compile(rf"\b({w})\b", flags=re.IGNORECASE).search

    missing = [p for p in existing_partitions if not bool(find_partition(p)(merge_cond))]

    if missing:
        raise ValueError(f"Missing partition conditions for: {missing}. Required: {existing_partitions}")

    if path_type in ("table", "table_name"):
        df.write.mode("overwrite").option("replaceWhere", merge_cond).format("delta").saveAsTable(path)
    else:
        df.write.mode("overwrite").option("replaceWhere", merge_cond).format("delta").save(path)


def overwrite(
    spark,
    df: DataFrame,
    path: str,
    path_type: str = "storage",
) -> None:
    """
    Full table overwrite.

    :param spark: SparkSession
    :param df: Source DataFrame
    :param path: Target Delta table path or catalog name
    :param path_type: 'storage' or 'table' (also accepts 'table_name')

    Example::
    overwrite(spark, df, "main.gold.flat_monthly_summary", "table")
    """
    _write_df(df, path, path_type, mode="overwrite")
