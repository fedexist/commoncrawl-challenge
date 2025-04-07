from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG('cc_pipeline_dag', 
         default_args=default_args, 
         schedule_interval=None,
         params={
            'year': '2025',
            'week': '13'
        }) as dag:

    # 1. Download segments
    download_segments = BashOperator(
        task_id='download_segments',
        bash_command='bash /opt/airflow/scripts/download_segments.sh {{ dag_run.conf["year"] }}-{{ dag_run.conf["week"] }}'
    )

    # 2. Extract links
    extract_links = BashOperator(
        task_id='extract_links',
        bash_command='python /opt/airflow/scripts/extract_links.py'
    )

    # 3. Load links to PostgreSQL
    load_links = BashOperator(
        task_id='load_links',
        bash_command='python /opt/airflow/scripts/load_links.py'
    )

    transform_and_analyze = BashOperator(
        task_id='transform_and_analyze',
        bash_command='python /opt/airflow/scripts/transform_and_analyze.py'
    )

    # 10. Save final results in Parquet
    save_parquet = BashOperator(
        task_id='save_parquet',
        bash_command='python /opt/airflow/dags/save_partitioned_parquet.py'
    )

    download_segments >> extract_links >> load_links >> transform_and_analyze >> save_parquet
