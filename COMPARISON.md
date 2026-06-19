# Comparison

`data-hub-gandalf` (imported as `gandalf`) is **not the first** library to do slowly changing
dimensions — SCD is a Kimball-era pattern (~1996) with many implementations across SQL, Spark,
and dbt. This page is an honest map of the landscape: who does which SCD types, and how rigorously
each is tested (real-engine end-to-end? on Spark? on Databricks?) — so you can pick the right tool,
and so we don't over-claim.

> Surveyed June 2026 by inspecting each project's repository (GitHub), docs, and PyPI. SCD-type
> support and "tested-on" columns reflect each project's own source/CI at that time — see **Sources**.

## SCD type coverage

Legend: ✅ supported · `—` not supported · ⚠️ recognized but not implemented.

| Library | Ecosystem | 0 | 1 | 2 | 3 | 4 | 6 | First release |
|---|---|:-:|:-:|:-:|:-:|:-:|:-:|---|
| **gandalf** (this) | PySpark + Delta over Spark Connect | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | 2026-06 |
| [mack](https://github.com/MrPowers/mack) | PySpark + Delta | — | — | ✅ | — | — | — | 2022-11 |
| [koheesio](https://github.com/Nike-Inc/koheesio) (Nike) | PySpark + Delta | — | ✅ | ✅ | — | — | — | 2024-05 |
| [hydro](https://github.com/christophergrant/hydro) | PySpark + Delta | — | ✅ | ✅ | — | — | — | 2023-01 |
| [fabricks](https://github.com/fabricks-framework/fabricks) | Databricks + PySpark + Delta | ✅ | ✅ | ✅ | — | — | — | 2024-08 |
| [dbxscd](https://github.com/maye-msft/SlowlyChangingDimensionsInDeltaLake) | Databricks + Delta | — | ✅ | ✅ | ✅ | — | — | 2021-08 |
| [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots) | dbt (multi-warehouse) | — | — | ✅ | — | — | — | 2019-07 |
| [Databricks DLT — AUTO CDC](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc) | Databricks (first-party) | — | ✅ | ✅ | — | — | — | 2022-02 |
| [Azure SCD-Merge-Wizard](https://github.com/Azure-Player/SCD-Merge-Wizard) | T-SQL / SQL Server | ✅ | ✅ | ✅ | ✅ | — | — | 2012-11 |

> gandalf recognizes the `scd2_cdc` alias but intentionally raises `NotImplementedError`
> (event-ordering / late-event semantics are not built), so its *practical* CDC story is no
> stronger than the tools above.

Among everything surveyed, **nothing else combined Type 4 (separate history table) and Type 6
(hybrid) with the rest of the range in a single library** — those two are rare in the wild.

## Test rigor

Does the project ship **automated tests that execute the SCD merge against a real engine** (vs mocks
or manual notebooks), and on what?

| Library | Real-engine E2E? | Engine in tests / CI | On real Databricks? |
|---|:-:|---|:-:|
| **gandalf** (this) | ✅ | Spark Connect + Delta (Dockerized, in CI); databricks-connect when configured | ⚠️ opt-in (not in default CI) |
| mack | ✅ | local Spark + Delta | — |
| koheesio | ✅ | local Spark (+ local Spark Connect) | — |
| hydro | ✅ | local Spark + Delta | — |
| **fabricks** | ✅ | Spark Connect + **real Databricks cluster** (Asset Bundle job) | ✅ |
| levi | ✅ | delta-rs (no Spark) | — |
| superlake / spark-fuse / odbc2deltalake | ✅ | local Spark + Delta | — |
| **dbt snapshots** (via dbt-databricks) | ✅ | **real Databricks** (functional tests) | ✅ |
| dbxscd / sahilbhange / others | — | manual notebooks, no automated tests | — |

## Where gandalf is — and isn't — distinctive

**Distinctive (uncommon combination):**

- **The full SCD `0/1/2/3/4/6` range in one merge orchestrator.** Most libraries do Type 2 (sometimes
  +Type 1); a few reach 1/2/3. We found nothing else covering Type 4 *and* Type 6 alongside the rest.
- **One test suite, two real targets.** The same E2E tests run against a zero-setup Dockerized Spark
  Connect + Delta server **or** a real Databricks cluster (databricks-connect), auto-selected.
- Opinionated, overridable SCD control columns (`SCDColumns`) plus built-in schema / duplicate /
  row-count guards inside `merge_delta_table`.

**Not distinctive — honest caveats:**

- **Not the first SCD library.** See the dates above — SQL Server (2012), dbt snapshots (2019),
  dbxscd (2021), mack & Databricks DLT (2022), and more all predate it.
- **Not the only one with real-engine E2E tests.** mack, koheesio, hydro, fabricks, and others run
  real-Spark integration tests that assert SCD output.
- **Not the only one tested on Databricks.** **fabricks** runs SCD integration tests on a real
  Databricks cluster (via a Databricks Asset Bundle job), and **dbt-databricks** runs snapshot
  (SCD2) functional tests against real Databricks. gandalf's Databricks path is opt-in and is **not**
  exercised by its default CI (which runs the Spark Connect + Docker path).
- **CDC is not implemented** — `scd2_cdc` raises `NotImplementedError`.

## Methodology & caveats

- Researched June 2026 via GitHub repository/code inspection and the web/PyPI (53 candidate
  tools examined; 21 confirmed to provide SCD merges). Dates are the earliest PyPI release or repo
  creation; capability and test claims come from each project's own source, docs, or CI.
- **"First of its kind" is not a claim this project makes.** Proving the absence of a prior library
  across the entire GitHub/PyPI long tail is not possible; this is a best-effort, exhaustive survey,
  not a proof.

## Sources

| Project | Link |
|---|---|
| mack | https://github.com/MrPowers/mack |
| koheesio (Nike) | https://github.com/Nike-Inc/koheesio |
| hydro | https://github.com/christophergrant/hydro |
| fabricks | https://github.com/fabricks-framework/fabricks |
| levi | https://github.com/mrpowers-io/levi |
| superlake | https://github.com/loicmagnien/superlake |
| spark-fuse | https://github.com/kevinsames/spark-fuse |
| odbc2deltalake | https://github.com/bmsuisse/odbc2deltalake |
| dbxscd | https://github.com/maye-msft/SlowlyChangingDimensionsInDeltaLake |
| dbt snapshots / dbt-databricks | https://docs.getdbt.com/docs/build/snapshots · https://github.com/dbt-labs/dbt-databricks |
| Databricks DLT — AUTO CDC | https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc |
| Azure SCD-Merge-Wizard | https://github.com/Azure-Player/SCD-Merge-Wizard |
| sahilbhange/spark-slowly-changing-dimension | https://github.com/sahilbhange/spark-slowly-changing-dimension |
