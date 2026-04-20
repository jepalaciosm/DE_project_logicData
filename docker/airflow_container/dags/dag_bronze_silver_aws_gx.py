from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


# Configuración básica del DAG
with DAG(
    dag_id="datalake_s3_to_glue_gx",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["aws", "glue", "s3"]
) as dag:

   
    # Ejecutar Glue Job
    run_glue_job_gx = GlueJobOperator(
        task_id="run_glue_job_gx",
        job_name="jepm-logicdata-dev-catalogo-etl-broze-silver",
        aws_conn_id="jepm_aws",
        region_name="us-east-1",  # ajusta si usas otra región
        wait_for_completion=True,
        script_args={
            "--env": "dev"
        }
    )

   
    # Orden de ejecución
    run_glue_job_gx