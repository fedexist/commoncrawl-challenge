FROM apache/airflow:slim-2.10.5-python3.12

USER root

RUN apt-get update -y && \
    apt-get install -y wget gzip

USER airflow
WORKDIR /home/airflow

RUN wget -qO- https://astral.sh/uv/install.sh | sh

COPY uv.lock uv.lock
COPY pyproject.toml pyproject.toml

RUN uv export --no-hashes --format requirements-txt > requirements.txt && \
    pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

