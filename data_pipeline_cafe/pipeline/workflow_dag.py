from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import sys

# Adiciona o diretório atual ao sys.path para importar o workflow.py
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Importa as funções do workflow.py
from workflow import (
    extract_dataset,
    extract_value_field,
    merge_dfs,
    first_question,
    second_question,
    last_question,
    export_to_parquet,
    load_to_bigquery
)

# Diretório para armazenar arquivos intermediários
INTERMEDIATE_DIR = "/opt/airflow/output"

def extract_data_task():
    os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
    df_event, df_customer, df_offer = extract_dataset()
    df_event = extract_value_field(df_event)
    df_event.to_parquet(os.path.join(INTERMEDIATE_DIR, "df_event.parquet"))
    df_customer.to_parquet(os.path.join(INTERMEDIATE_DIR, "df_customer.parquet"))
    df_offer.to_parquet(os.path.join(INTERMEDIATE_DIR, "df_offer.parquet"))

def analyze_data_task():
    df_event = pd.read_parquet(os.path.join(INTERMEDIATE_DIR, "df_event.parquet"))
    df_customer = pd.read_parquet(os.path.join(INTERMEDIATE_DIR, "df_customer.parquet"))
    df_offer = pd.read_parquet(os.path.join(INTERMEDIATE_DIR, "df_offer.parquet"))
    dfs_total = merge_dfs(df_event, df_customer, df_offer)
    first_question(dfs_total)
    second_question(dfs_total, df_customer)
    last_question(dfs_total)

def export_and_load_task():
    export_to_parquet()
    load_to_bigquery()

with DAG(
    dag_id="workflow_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "bigquery"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_task
    )

    task_analyze = PythonOperator(
        task_id="analyze_data",
        python_callable=analyze_data_task
    )

    task_export_load = PythonOperator(
        task_id="export_and_load",
        python_callable=export_and_load_task
    )

    task_extract >> task_analyze >> task_export_load
