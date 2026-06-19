from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    lit,
    sha2,
    when,
    xxhash64,
)
from pyspark.sql.types import StringType

from gandalf.scd.columns import SCDColumns


def hash_columns(*columns: Column, separator: str = "||") -> Column:
    """
    Generate a SHA-256 hash Column expression over specific columns.

    Returns a Column expression — not a DataFrame — so it works anywhere a
    column expression is valid: ``withColumn()``, ``select()``,
    ``when().otherwise()``, etc.

    NULLs and empty strings are replaced with ``'*'`` before hashing.
    The separator (default ``"||"``) prevents hash collisions between inputs
    like ``("a", "bc")`` and ``("ab", "c")``.

    This is the native-Spark replacement for the notebook-level ``hash_udf``,
    eliminating the Python UDF serialisation overhead with vectorised Spark SQL.

    .. note::
        The legacy ``hash_udf`` concatenated values with no separator, so it
        could produce identical hashes for different inputs. Pass
        ``separator=""`` only if you need byte-for-byte compatibility with
        existing ``hash_udf``-generated values in a Delta table.

    :param columns: One or more Column expressions to hash.
    :param separator: String placed between values before hashing (default ``"||"``).
    :return: Column expression producing a 64-character lowercase SHA-256 hex string.

    Example::

        # single column — entity natural key
        df.withColumn("party_id", hash_columns(col("cpf")))

        # composite key
        df.withColumn("billing_store_id", hash_columns(col("pjid"), col("industry_id")))

        # inside a conditional expression
        df.withColumn("store_id", when(col("store_id").isNotNull(), col("store_id"))
                                   .otherwise(hash_columns(col("pjid"))))

        # with pre-concatenated literals
        df.withColumn("order_id", hash_columns(concat(col("order_id"), lit("order"))))
    """
    null_safe = [
        when(c.isNull() | (c.cast(StringType()) == lit("")), lit("*")).otherwise(c.cast(StringType())) for c in columns
    ]
    return sha2(concat_ws(separator, *null_safe), 256)


def generate_check_sum(
    df: DataFrame,
    control_cols: list | None = None,
    tracked_columns: list | None = None,
    checksum_col: str | None = None,
    scd_control_cols: frozenset[str] | set[str] | None = None,
) -> DataFrame:
    """
    Generate SHA-256 checksum over deterministic business columns.

    Columns are sorted alphabetically for determinism across runs. NULLs are
    replaced with ``'*'`` before concatenation. SCD control columns and all SK
    columns (sk_*) are excluded automatically unless ``tracked_columns`` is
    supplied, in which case only tracked columns are hashed.

    :param df: Source DataFrame
    :param control_cols: Additional columns to exclude from the hash
    :param tracked_columns: Optional explicit list of columns to hash
    :param checksum_col: Output checksum column name (default: ``SCDColumns().checksum``)
    :param scd_control_cols: SCD control-column names to exclude from the hash
        (default: the four ``SCDColumns()`` defaults)
    :return: DataFrame with checksum column added
    """
    control_cols = control_cols or []
    if checksum_col is None:
        checksum_col = SCDColumns().checksum
    scd_control_cols = set(scd_control_cols) if scd_control_cols is not None else set(SCDColumns().as_tuple())
    if tracked_columns is not None:
        missing = sorted(set(tracked_columns) - set(df.columns))
        if missing:
            raise ValueError(f"tracked_columns missing from DataFrame: {missing}")
        cols_to_hash = sorted(tracked_columns)
    else:
        ignore = scd_control_cols | set(control_cols) | {c for c in df.columns if c.startswith("sk_")}
        cols_to_hash = sorted([c for c in df.columns if c not in ignore])
    hash_cols = [
        when(col(c).isNull() | (col(c).cast(StringType()) == lit("")), lit("*")).otherwise(col(c).cast(StringType()))
        for c in cols_to_hash
    ]
    return df.withColumn(checksum_col, sha2(concat_ws("||", *hash_cols), 256))


def generate_dim_sk(df: DataFrame, ids: list, sk_name: str, checksum_col: str | None = None) -> DataFrame:
    """
    Generate a Dimension Surrogate Key using SHA-256.

    Key is unique per SCD2 version because it includes the checksum + current_timestamp.
    Requires the checksum column to be present (call generate_check_sum first).

    :param df: DataFrame with the checksum column already present
    :param ids: Business key column names
    :param sk_name: Name for the generated SK column (convention: sk_{entity})
    :param checksum_col: Checksum column name to fold into the key
        (default: ``SCDColumns().checksum``)
    :return: DataFrame with SK column added

    Example::
    generate_dim_sk(df, ["id_client"], "sk_client")
    """
    if checksum_col is None:
        checksum_col = SCDColumns().checksum
    hash_cols = [coalesce(col(c).cast(StringType()), lit("*")) for c in ids]
    hash_cols += [col(checksum_col), current_timestamp().cast(StringType())]
    return df.withColumn(sk_name, sha2(concat_ws("||", *hash_cols), 256))


def generate_fact_sk(df: DataFrame, ids: list, sk_name: str) -> DataFrame:
    """
    Generate a Fact Surrogate Key using xxhash64.

    Key is deterministic: same input always produces the same key.
    Result is cast to StringType for consistency with dim SK column type.

    :param df: DataFrame
    :param ids: Business key column names
    :param sk_name: Name for the generated SK column (convention: sk_{entity})
    :return: DataFrame with SK column added

    Example::
    generate_fact_sk(df, ["id_order", "id_client"], "sk_order")
    """
    return df.withColumn(sk_name, xxhash64(*[col(c) for c in ids]).cast(StringType()))
