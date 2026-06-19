# Poetry-driven test + lint image for gandalf.
#
# Used by the `integration-tests` and `lint-formatter` docker-compose services.
# Mirrors the tooling convention from tinuvi/opentelemetry-instrumentation-django-q2-full-of-juice
# (poetry install into the system interpreter, repo bind-mounted at runtime), adapted for
# gandalf's Spark stack.
#
# The Spark Connect client (tests/conftest.py -> sc://) is pure Python, but the in-process
# local[*] Spark used by the unit tests (tests/unit/) needs a JVM, so a headless JRE 17 is
# installed to let the FULL suite run in one image.
FROM python:3.12

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=2.4.1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

# Java for the local[*] in-process Spark (unit tests). The Debian base ships OpenJDK 21,
# which Spark 4.0 supports. procps gives `ps`, which the Spark launcher scripts rely on.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# Arch-independent JAVA_HOME (works on amd64 and arm64 base images).
ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" "$JAVA_HOME"

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "poetry==${POETRY_VERSION}"

# Dependency layer: copy the manifests so the install layer caches until deps change. README.md
# and LICENSE are included because the [project] table references them by file (readme / license),
# and poetry reads that metadata during `poetry install` — they must exist in this layer.
COPY pyproject.toml poetry.lock README.md LICENSE ./

# Install the dev group (the Spark Connect test toolchain: pyspark[connect], delta-spark, etc.).
# It is a non-optional group, so a plain `poetry install` pulls it in. databricks-connect is
# intentionally NOT a dependency here — it collides with pyspark on the `pyspark` import
# namespace; the databricks-connect path is a manual, runtime-only opt-in (see README).
RUN poetry install --no-root

COPY . ./

# Install gandalf itself (editable, no deps) so `import gandalf` resolves regardless of CWD
# and importlib.metadata can see the dist-info. At runtime the repo is bind-mounted over
# /app, so this points at the live source.
RUN pip install --no-deps -e .
