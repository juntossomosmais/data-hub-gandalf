# SDLC rules

Imperative guidance for working on the project. Follow these end-to-end on every change.

## Code style

- Write a test for every implementation change. No exception for "trivial" fixes.

## Updating poetry settings

- Update `poetry.lock` when `pyproject.toml` changes:
    ```bash
    docker compose run --rm --remove-orphans integration-tests poetry lock -v
    ```
- Build the Docker images to reflect the changes:
    ```bash
    docker compose build integration-tests lint-formatter
    ```

## Testing

- **Selective runs**: pass a pytest path/node to the integration-tests container (it boots the Spark Connect server first):
    ```bash
    docker compose run --rm integration-tests pytest tests/test_merge_delta_table.py -v
    ```
- **Coverage for a specific source file**: scope the report to the module you care about:
    ```bash
    docker compose run --rm integration-tests bash -c "coverage run --source=gandalf -m pytest tests/ && coverage report --include=gandalf/merge/merge_delta_table.py"
    ```
- Run the full library suite before declaring a change complete:
    ```bash
    docker compose run --rm integration-tests
    ```

### Writing tests

- Only use the `unittest` framework that comes with Python and what is natively supported by PySpark.
- Use the Arrange-Act-Assert (AAA) pattern.
- The test class should inherit from the class `PySparkTestCase` defined in the file `./tests/resources/utils.py`. Import it as `from tests.resources.utils import PySparkTestCase`.
- Use the assert utilities `assertDataFrameEqual` and `assertSchemaEqual` defined in the module `pyspark.testing.utils`.
- `assertSchemaEqual` expected the dataframe schema. A valid call would be `assertSchemaEqual(actualDf.schema, expectedDf.schema)`
- Use `pyspark.sql.DataFrame.collect()` to get the actual data from a dataframe.
- When comparing rows, convert the collected data to a list of dictionaries for easier comparison. For example:
  ```
  actual_data = [row.asDict() for row in actualDf.collect()]
  ```

## Lint & format

- Run after the implementation is complete (no need to re-run tests after):
    ```bash
    docker compose run --remove-orphans --rm lint-formatter
    ```

## Documentation

- Update `CHANGELOG.md` only when `./gandalf/` (the library source) changes, but not including the `tests` folder. Use the active `[X.Y.Z]` heading and `Added` / `Changed` / `Fixed` / `Removed` subsections. Skip it for repo-tooling or sample-project edits.
- Update `README.md` when the public API, install steps, or supported Python / Databricks Runtime versions change.
- Do **not** create new top-level docs (`*.md`) unless explicitly asked.

## Commits

- Use Conventional Commits.

## Deployment

- Published to PyPI as **`data-hub-gandalf`** (imported as `gandalf`) by `.github/workflows/publish-package.yml`, which triggers on a semver git tag (`X.Y.Z`) or manual dispatch.
- Versioning is **git-tag-driven**: `pyproject.toml` keeps a `0.0.0` placeholder under `[project].version`, and the workflow runs `poetry version $TAG_NAME` at publish time — do **not** hand-edit the version. Cut a release by pushing a tag, e.g. `git tag 0.2.0 && git push origin 0.2.0`.
- Requires a `PYPI_TOKEN` repository secret (set it in GitHub settings before the first release).
- For air-gapped installs, the wheel can still be built (`poetry build`) and uploaded to a Databricks Unity Catalog volume.

## Tips

### User Defined Functions (UDF)

- A UDF function ends with `_udf`. The function `hash_udf` is a valid function. Sample
  ```python
  from slugify import slugify
  from pyspark.sql.types import StringType
  from pyspark.sql.functions import udf
  
  
  def _slugify(text: str) -> str | None:
      if text is None:
          return None
  
      return slugify(text)
  
  
  slugify_text_udf = udf(_slugify, StringType())
  ```