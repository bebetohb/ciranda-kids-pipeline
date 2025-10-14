from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# 🛠️ Garante que o Airflow encontre os scripts
sys.path.append('/opt/airflow/etl')

# 📦 Importa as funções principais de cada etapa
import extract_load
import transform
import load_postgres

default_args = {
    'owner': 'humberto',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline_ciranda_kids',
    default_args=default_args,
    description='Pipeline ETL completa: XLSX → CSV → Transform → PostgreSQL',
    start_date=datetime(2025, 10, 14),
    schedule_interval='@daily',  # ⏰ Executa diariamente
    catchup=False,
    tags=['etl', 'ciranda_kids'],
) as dag:

    def run_extract_load():
        extract_load.main()

    def run_transform():
        transform.main()

    def run_load_postgres():
        conn = load_postgres.get_connection()
        load_postgres.create_tables(conn)
        load_postgres.load_data_to_postgres(conn)
        conn.close()

    extract_task = PythonOperator(
        task_id='extract_and_load_csv',
        python_callable=run_extract_load
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=run_transform
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=run_load_postgres
    )

    # 🔗 Define a ordem das tarefas
    extract_task >> transform_task >> load_task