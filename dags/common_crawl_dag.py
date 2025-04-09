from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
}

cwd = "/opt/airflow/"

with DAG(
    "cc_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    params={"year": "2025", "week": "13"},
) as dag:
    # 1. Download segments
    download_segments = BashOperator(
        task_id="download_segments",
        bash_command='bash /opt/airflow/scripts/download_segments.sh {{ dag_run.conf["year"] }}-{{ dag_run.conf["week"] }}',
        cwd=cwd,
    )

    # 2. Extract links
    extract_links = BashOperator(
        task_id="extract_links",
        bash_command="python /opt/airflow/scripts/read_warc_async.py",
        env={
            "SEGMENTS_FOLDER": "/opt/airflow/commoncrawl/segments"
        },
        cwd=cwd,
    )

    # 3. Load links to PostgreSQL
    load_links = BashOperator(
        task_id="load_links",
        bash_command="python /opt/airflow/scripts/load_links.py",
        cwd=cwd,
        env={
            "POSTGRES_HOST": "postgres",
        },
    )

    # 4. Process the links and save to parquet
    transform_and_analyze = BashOperator(
        task_id="transform_and_analyze",
        bash_command="python /opt/airflow/scripts/transform_and_analyze.py",
        cwd=cwd,
    )

    download_segments >> extract_links >> load_links >> transform_and_analyze
