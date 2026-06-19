from delta.tables import DeltaTable
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count

from gandalf.exceptions import (
    DuplicatedDataFrameError,
    InvalidColumnsError,
    InvalidTable,
    RowCountException,
)
from gandalf.scd.columns import SCDColumns
from gandalf.scd.constants import SCD2, SCD2_CDC, SCD2_NO_DELETE, SCD3, SCD4, SCD6
from gandalf.scd.predicates import quote_identifier


def _read_target(spark, path: str, path_type: str) -> DataFrame:
    """Read target Delta table. Accepts both storage paths and Unity Catalog names."""
    if path_type in ("table", "table_name"):
        return spark.read.table(path)
    return spark.read.format("delta").load(path)


def check_duplicates(df: DataFrame, ids: list, current_flag: bool, current_flag_col: str | None = None) -> None:
    """
    Check if DataFrame contains duplicated rows grouped by ids.

    :param df: DataFrame to check
    :param ids: Column list to group values
    :param current_flag: If True, filter current rows before checking (SCD tables)
    :param current_flag_col: Name of the current-flag column (default: ``SCDColumns().is_current``)
    :raises DuplicatedDataFrameError: when duplicates are found

    Example::
    check_duplicates(df, ["id_client"], False)
    check_duplicates(target_df, ["id_client"], True)  # SCD current rows only
    """
    flag_col = current_flag_col or SCDColumns().is_current
    df_check = df.filter(f"{quote_identifier(flag_col)} = 1") if current_flag else df
    df_dupes = df_check.groupBy(ids).agg(count("*").alias("count")).filter("count > 1")
    if df_dupes.count() != 0:
        sample = df_dupes.select(ids).limit(10).collect()
        raise DuplicatedDataFrameError("\n".join(map(str, sample)))


def table_check(spark, path: str, path_type: str = "storage") -> None:
    """
    Verify that path points to a valid Delta table.

    :param spark: SparkSession
    :param path: ADLS path or Unity Catalog name (catalog.schema.table)
    :param path_type: 'storage' for ADLS path, 'table' or 'table_name' for catalog name
    :raises InvalidTable: when the table does not exist

    Example::
    table_check(spark, "main.silver.client", "table")
    table_check(spark, "abfss://<container>@<account>.dfs.core.windows.net/client", "storage")
    """
    try:
        if path_type in ("table", "table_name"):
            DeltaTable.forName(spark, path)
        else:
            if not DeltaTable.isDeltaTable(spark, path):
                raise InvalidTable()
    except InvalidTable:
        raise
    except Exception:
        raise InvalidTable()


def column_check(
    spark,
    df: DataFrame,
    path: str,
    path_type: str = "storage",
    sk_name: str = "",
    create_part_col: str = "",
    part_name: str = "",
    col_cdc_update_time: str = "",
    extra_target_exclude: set[str] | None = None,
    scd_control_cols: frozenset[str] | set[str] | None = None,
) -> None:
    """
    Compare source and target columns, reporting all divergences.

    SCD control columns, SK columns (sk_*), and the explicit sk_name are excluded
    from the target side before comparison. The CDC timestamp column is excluded
    from the source side.

    :param spark: SparkSession
    :param df: Source DataFrame
    :param path: Target Delta table path or catalog name
    :param path_type: 'storage' or 'table' (also accepts 'table_name')
    :param sk_name: SK column name to exclude from target (auto-excluded via sk_* wildcard)
    :param create_part_col: Source column that will become a partition (exclude from check)
    :param part_name: Partition column name on target to exclude
    :param col_cdc_update_time: CDC timestamp column to exclude from source
    :param scd_control_cols: SCD control-column names to exclude from the target
        (default: the four ``SCDColumns()`` defaults)
    :raises InvalidColumnsError: on schema mismatch

    Example::
    column_check(spark, df, "main.silver.client", "table")
    """
    df_target = _read_target(spark, path, path_type)

    scd_control = set(scd_control_cols) if scd_control_cols is not None else set(SCDColumns().as_tuple())
    part_col_to_exclude = part_name if create_part_col else ""
    exclude_from_target = (
        scd_control
        | {sk_name, part_col_to_exclude}
        | {c for c in df_target.columns if c.startswith("sk_")}
        | (extra_target_exclude or set())
    )
    exclude_from_source = {col_cdc_update_time} if col_cdc_update_time else set()

    target_dict = {n: t for n, t in df_target.dtypes if n not in exclude_from_target}
    source_dict = {n: t for n, t in df.dtypes if n not in exclude_from_source}

    target_names = set(target_dict)
    source_names = set(source_dict)

    missing_in_source = target_names - source_names
    extra_in_source = source_names - target_names
    schema_diffs = [
        f"Schema divergence — column '{n}': source={source_dict[n]}, target={target_dict[n]}"
        for n in source_names & target_names
        if source_dict[n] != target_dict[n]
    ]

    if missing_in_source or extra_in_source or schema_diffs:
        msg = ""
        if missing_in_source:
            msg += f"\nColumns in target but not in source: {sorted(missing_in_source)}"
        if extra_in_source:
            msg += f"\nColumns in source but not in target: {sorted(extra_in_source)}"
        if schema_diffs:
            msg += "\n" + "\n".join(schema_diffs)
        raise InvalidColumnsError(msg)


def row_count(df_rows: int, target_df: DataFrame, current_flag_col: str | None = None) -> None:
    """
    Validate that source row count matches current rows in target after merge.

    :param df_rows: Source DataFrame row count (pre-merge)
    :param target_df: Target Delta DataFrame (post-merge)
    :param current_flag_col: Name of the current-flag column (default: ``SCDColumns().is_current``)
    :raises RowCountException: when counts do not match

    Example::
    row_count(df.count(), spark.read.table("main.silver.client"))
    """
    flag_col = current_flag_col or SCDColumns().is_current
    target_rows = target_df.filter(f"{quote_identifier(flag_col)} = 1").count()
    if df_rows != target_rows:
        raise RowCountException(f"\nSource: {df_rows}\nTarget: {target_rows}")


def validate_required_columns(df: DataFrame, columns: list[str], context: str) -> None:
    """Validate that a DataFrame contains all required columns."""
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"{context} missing required columns: {missing}")


def validate_scd_source_columns(df: DataFrame, config) -> None:
    """Validate source columns needed by an SCD strategy."""
    required = list(config.ids)
    if config.tracked_columns:
        required.extend(config.tracked_columns)
    if config.protected_columns:
        required.extend(config.protected_columns)
    if config.scd_type in {SCD3, SCD6}:
        required.extend(config.previous_columns.keys())
    if config.scd_type == SCD2_CDC:
        required.append(config.cdc_timestamp_col)
    validate_required_columns(df, required, "source")


def validate_scd_target_columns(target_df: DataFrame, config) -> None:
    """Validate target columns needed by an SCD strategy."""
    required = list(config.ids)
    if config.scd_type in {SCD2, SCD2_NO_DELETE, SCD2_CDC, SCD6}:
        required.extend(
            [
                config.checksum_col,
                config.current_flag_col,
                config.effective_from_col,
                config.effective_to_col,
            ]
        )
    if config.scd_type in {SCD3, SCD6}:
        required.extend(config.previous_columns.keys())
        required.extend(config.previous_columns.values())
    validate_required_columns(target_df, required, "target")
    if config.scd_type == SCD4 and not config.history_path:
        raise ValueError("SCD4 target validation requires history_path")


def validate_no_duplicate_current(target_df: DataFrame, ids: list[str], current_flag_col: str) -> None:
    """Validate no duplicate current rows exist for SCD history tables."""
    validate_required_columns(target_df, [*ids, current_flag_col], "target")
    df_current = target_df.filter(f"{quote_identifier(current_flag_col)} = 1")
    df_dupes = df_current.groupBy(ids).agg(count("*").alias("count")).filter("count > 1")
    if df_dupes.count() != 0:
        sample = df_dupes.select(ids).limit(10).collect()
        raise DuplicatedDataFrameError("\n".join(map(str, sample)))
