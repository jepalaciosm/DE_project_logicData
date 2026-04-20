from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_s3_files(ti):
    files = ti.xcom_pull(task_ids='list_s3_files')
    print("Archivos en S3:")
    print(files)

# Configuración básica del DAG
with DAG(
    dag_id="datalake_s3_to_glue",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["aws", "glue", "s3"]
) as dag:

    # Listar archivos en el bucket (capa bronze)
    list_s3_files = S3ListOperator(
        task_id="list_s3_files",
        bucket="jepm-logicdata-dev-bronze-067330477391",
        prefix="",  # puedes poner algo como "sales/" si quieres filtrar
        aws_conn_id="jepm_aws"
    )

    # Ejecutar Glue Job
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="jepm-logicdata-dev-catalogo-etl",
        aws_conn_id="jepm_aws",
        region_name="us-east-1",  # ajusta si usas otra región
        wait_for_completion=True,
        script_args={
            "--env": "dev"
        }
    )

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

    print_files = PythonOperator(
        task_id="print_files",
        python_callable=print_s3_files
    )

    # Orden de ejecución
    list_s3_files >> print_files >> run_glue_job