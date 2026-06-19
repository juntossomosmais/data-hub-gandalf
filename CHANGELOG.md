# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-19

### Added

- `merge_delta_table` orchestrator: validates the target Delta table, optionally filters incrementally (`delta_col_filter`), runs duplicate / schema / row-count guards, generates checksum and surrogate keys, casts column types to the target schema, and dispatches to the chosen merge strategy. Parameters after `merge_type` are keyword-only.
- SCD merge strategies: Type 0 (`scd0`), Type 1 (`scd1` / `scd1_delete`), Type 2 (`scd2` / `scd2_no_delete`), Type 3 (`scd3`), Type 4 (`scd4`), and Type 6 (`scd6`), plus `overwrite` and `overwrite-partition`. `scd2_cdc` is recognized but intentionally blocked (raises `NotImplementedError`).
- `SCDColumns` value object naming the four control columns gandalf manages, with opinionated `scd_`-namespaced defaults (`scd_start_dt`, `scd_end_dt`, `scd_is_current`, `scd_checksum`). Override via the `scd_columns` parameter (or `SCDConfig.columns`) to point gandalf at a table that already uses different control-column names — the names are threaded through the merge engine and the post-merge validators.
- `SCDConfig` for the advanced SCD types (`scd0`/`scd3`/`scd4`/`scd6`), observability (`return_metrics`), and custom control-column names. These advanced types are reachable only through `scd_config`; the common types (`scd`, `scd_no_delete`, `upsert`, `overwrite`, `overwrite-partition`) work from the flat parameters.
- Surrogate-key and checksum utilities: `hash_columns`, `generate_check_sum` (writes the configured checksum column), `generate_dim_sk` (SHA-256, unique per SCD version), `generate_fact_sk` (xxhash64, deterministic).
- Validation helpers: `check_duplicates`, `column_check`, `row_count`, `table_check`, and the SCD source/target column validators. `merge_delta_table` raises `InvalidColumnsError` when an existing target is missing the configured SCD control columns.
- SCD2 safety guards: an existing target's checksum column must be `StringType` and its current-flag column must be an integer (1/0) type; source columns may not collide with the internal `merge_key` staging columns; and SCD control-column names must be distinct from `ids` and `previous_columns` — all raise a clear error before any Delta write.
- SCD6 fails fast with `DuplicatedDataFrameError` when the target already holds duplicate current rows for a key, instead of surfacing a Delta multiple-source-rows-matched error.
- `overwrite-partition` rejects `merge_cond` predicates containing `OR` / `IS NOT NULL`, which could widen the `replaceWhere` and drop unintended partitions.
- All generated SQL predicates are built with backtick-quoted, validated identifiers; `merge_cond` is documented as trusted-input-only (interpolated verbatim into the Delta MERGE / filter).
