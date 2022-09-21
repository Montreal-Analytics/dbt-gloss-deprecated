FROM python:3.8-slim

WORKDIR /work
COPY .github/.pre-commit-config-action.yaml .pre-commit-config.yaml

RUN apt-get update &&\
    apt-get upgrade -y && \
    apt-get install -y git && \
    pip install --no-cache-dir pre-commit==2.18.0 \
    dbt-core==1.0.0 \
    dbt-postgres==1.0.0 \
    dbt-redshift==1.0.0 \
    dbt-snowflake==1.0.0 \
    dbt-bigquery==1.0.0 && \
    git init && \
    pre-commit install-hooks && \
    apt-get clean autoclean && \
    apt-get autoremove --yes  && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    rm -rf /work

WORKDIR /github/workspace

ENTRYPOINT [ "pre-commit" ]
