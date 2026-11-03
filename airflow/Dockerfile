FROM astrocrpublic.azurecr.io/runtime:3.3-11

# dbt lives in its own virtualenv, so its pins and Airflow's never have to agree.
# (Same versions as the repo's pyproject.toml; native arm64 wheels on Apple Silicon.)
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core~=1.12.6" "dbt-snowflake~=1.12.4" "dbt-duckdb~=1.11.1" "duckdb>=1.4,<1.5"
ENV DBT_BIN=/usr/local/airflow/dbt_venv/bin/dbt

# Nothing from the repo is copied in here beyond this folder (dags/, include/, tests/). The recon code,
# the dbt project, the reference FX rates and the Snowflake keys are mounted at runtime by
# docker-compose.override.yml. The keys are never in the image.
