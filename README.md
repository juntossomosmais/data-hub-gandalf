# Data Hub Gandalf

Python library for Databricks data pipelines — SCD Types 0/1/2/3/4/6, upserts, surrogate keys, checksums, and schema validations. You shall not pass... bad data.

## Installation

Published on PyPI as **`data-hub-gandalf`** (imported as `gandalf`):

```bash
pip install data-hub-gandalf
```

In a Databricks notebook or cluster:

```python
%pip install data-hub-gandalf
```

For an offline / air-gapped install, build the wheel with Poetry and install it from a Unity Catalog volume:

```bash
poetry build            # -> dist/data_hub_gandalf-<version>-py3-none-any.whl
```

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/data_hub_gandalf-<version>-py3-none-any.whl
```

## Quickstart

```python
from gandalf import merge_delta_table

merge_delta_table(
    spark=spark,
    df=df,
    path="main.gold.dim_client",
    ids=["id_client"],
    merge_type="scd",       # SCD Type 2, full snapshot — see Capabilities
    sk_type="dim",
    sk_name="sk_client",
)
```

## Capabilities

A scan-map of the merge strategies and helpers gandalf provides. Legend: ✅ supported · ⚙️ automatic · ⚠️ supported with a caveat · `—` not applicable.

| Capability | `merge_type` (legacy alias) | Status | History | Deletes | Notes |
|---|---|:---:|---|---|---|
| SCD Type 2 — full snapshot | `scd2` (`scd`) | ✅ | row versions | logical | requires all source rows each run; row count validated |
| SCD Type 2 — incremental | `scd2_no_delete` (`scd_no_delete`) | ✅ | row versions | — | incremental source; absent keys are not closed |
| SCD Type 2 — CDC | `scd2_cdc` | ⚠️ blocked | — | — | event-ordering semantics not implemented; raises `NotImplementedError` |
| SCD Type 1 — upsert | `scd1` (`upsert`) | ✅ | — | — | overwrite current value |
| SCD Type 1 — upsert + delete | `scd1_delete` (`upsert-delete`) | ✅ | — | hard | also deletes rows absent from source |
| SCD Type 0 — insert-only | `scd0` | ✅ | — | — | insert new keys only; protected columns cannot change |
| SCD Type 3 — previous values | `scd3` | ✅ | previous-value columns | — | requires `previous_columns` |
| SCD Type 4 — history table | `scd4` | ✅ | separate history table | configurable | requires `history_path` |
| SCD Type 6 — hybrid | `scd6` | ✅ | row versions + prev columns | configurable | requires `previous_columns` |
| Full overwrite | `overwrite` | ✅ | — | — | replaces the whole table |
| Partition overwrite | `overwrite-partition` | ✅ | — | — | `replaceWhere`-scoped replacement |
| Checksum change detection | — | ⚙️ | — | — | `generate_check_sum` over business columns; powers SCD2/4/6 |
| Surrogate keys | — | ✅ | — | — | `generate_dim_sk` (SHA-256, unique per version) / `generate_fact_sk` (xxhash64, deterministic) |
| Schema & duplicate guards | — | ⚙️ | — | — | `column_check`, `check_duplicates`, `table_check`, `row_count` run inside `merge_delta_table` |
| Auto column-type casting | — | ⚙️ | — | — | source columns cast to the target schema before merge |

SCD history strategies manage four control columns — `scd_start_dt`, `scd_end_dt`, `scd_is_current`, and `scd_checksum` — automatically. Point gandalf at a table that uses different names by passing `scd_columns=SCDColumns(is_current="current_flag", checksum="check_sum")`.

---

## What it does

### `merge_delta_table`

Persists a Spark DataFrame into a Delta table. Internally it:

1. Validates the target Delta table exists
2. Filters incoming data incrementally via `delta_col_filter` (optional)
3. Detects duplicates in the source
4. Validates column schema against the target
5. Generates checksum for change detection
6. Generates surrogate key (if `sk_type` is set)
7. Auto-casts column types to match target schema
8. Executes the chosen merge strategy
9. Validates row count after merge

```python
from gandalf import merge_delta_table

merge_delta_table(
    spark=spark,
    df=df,
    path="main.gold.dim_client",
    ids=["id_client"],
    merge_type="scd",       # see strategies below
    sk_type="dim",
    sk_name="sk_client",
)
```

**Merge strategies — canonical names (legacy aliases accepted):**

| `merge_type` | Legacy alias | SCD type | History | Deletes | Notes |
|---|---|---|---|---|---|
| `scd2` | `scd` | Type 2 | Row versions | Logical | Full snapshot; requires all source rows |
| `scd2_no_delete` | `scd_no_delete` | Type 2 | Row versions | No | Incremental source |
| `scd2_cdc` | — | Type 2 | Row versions | Yes | CDC event ordering — explicitly blocked, raises `NotImplementedError` |
| `scd1` | `upsert` | Type 1 | No | No | Overwrite current value |
| `scd1_delete` | `upsert-delete` | Type 1 | No | Hard | Overwrite + delete absent rows |
| `scd0` | — | Type 0 | No | No | Insert new keys only; protected columns cannot change |
| `scd3` | — | Type 3 | Previous value columns | No | Requires `previous_columns` config |
| `scd4` | — | Type 4 | Separate history table | Configurable | Requires `history_path` config |
| `scd6` | — | Type 6 | Row versions + prev columns | Configurable | Requires `previous_columns` config |
| `overwrite` | — | — | No | N/A | Full table replacement |
| `overwrite-partition` | — | — | No | N/A | Partition-scoped replacement |

**Advanced strategies (`scd0`/`scd3`/`scd4`/`scd6`) are configured through `SCDConfig`:**

```python
from gandalf import SCDColumns, SCDConfig, merge_delta_table

# SCD Type 3 — previous-value columns
merge_delta_table(
    spark, df, "catalog.schema.dim_client", ["id_client"],
    scd_config=SCDConfig(scd_type="scd3", ids=["id_client"],
                         previous_columns={"address": "previous_address"}),
)

# SCD Type 4 — current table + history table
merge_delta_table(
    spark, df, "catalog.schema.current_client", ["id_client"],
    scd_config=SCDConfig(scd_type="scd4", ids=["id_client"],
                         history_path="catalog.schema.hist_client"),
)

# SCD Type 6 — Type 2 rows + previous-value columns
merge_delta_table(
    spark, df, "catalog.schema.dim_client", ["id_client"],
    scd_config=SCDConfig(scd_type="scd6", ids=["id_client"],
                         previous_columns={"address": "previous_address"}),
)
```

`SCDConfig` also carries the control-column names (`columns=SCDColumns(...)`), `return_metrics`, and `tracked_columns`. Pass either `scd_columns` **or** `scd_config` — not both.

---

### Utilities

```python
from gandalf import generate_check_sum, generate_dim_sk, generate_fact_sk, hash_columns

hash_columns(*cols)                    # SHA-256 Column expr for withColumn/select/when
generate_check_sum(df, control_cols)   # adds scd_checksum over all business columns
generate_dim_sk(df, ids, sk_name)      # SHA-256 surrogate key (unique per SCD version)
generate_fact_sk(df, ids, sk_name)     # xxhash64 surrogate key (deterministic)
```

| Function | Description |
|---|---|
| `hash_columns(*cols)` | SHA-256 Column expression — use inside `withColumn`, `select`, `when` |
| `generate_check_sum(df, control_cols)` | Adds the `scd_checksum` column over all business columns |
| `generate_dim_sk(df, ids, sk_name)` | SHA-256 surrogate key — unique per SCD version |
| `generate_fact_sk(df, ids, sk_name)` | xxhash64 surrogate key — deterministic across runs |

---

## Data protection guidance

- Prefer checksum comparisons over blind updates so reruns remain idempotent.
- Validate source-vs-target schemas before Delta `MERGE`.
- Use deterministic surrogate keys for dimensions and facts.
- Keep partition overwrites constrained by explicit predicates.
- Avoid embedding notebook-only globals inside core library logic; pass dependencies as function arguments.