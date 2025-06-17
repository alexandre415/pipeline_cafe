from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import logging

# Importar as funções wrapper do seu novo arquivo tasks_wrapper.py
from tasks_wrapper import (
    task_full_extraction_and_merge,
    task_run_analyses,
    task_load_to_bigquery_final
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Variáveis de ambiente para o BigQuery (para serem passadas ao PythonOperator, se necessário)
# Elas são lidas diretamente dentro das tasks_wrapper via os.getenv, mas é bom ter aqui para clareza
# e como fallback se o os.getenv na wrapper falhar (no caso do seu workflow.py original)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-default-gcp-project-id")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "your_default_dataset_id")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='data_pipeline_cafe_original_workflow_dag', # Novo ID para este fluxo específico
    default_args=default_args,
    description='DAG calling functions from original workflow.py with minimal changes',
    schedule_interval=None,
    tags=['data_pipeline', 'original_workflow', 'cafe'],
    catchup=False,
) as dag:

    # Task 1: Extração e Fusão dos DataFrames Iniciais
    # Esta task vai gerar 'dfs_total.parquet' e 'df_customer.parquet' no diretório de saída
    extract_and_merge_task = PythonOperator(
        task_id='extract_and_merge_initial_data',
        python_callable=task_full_extraction_and_merge,
        # Não precisamos passar argumentos diretamente aqui, pois a task_wrapper já sabe onde salvar
    )

    # Task 2: Execução das Análises (first_question, second_question, last_question) e export_to_parquet
    # Esta task vai ler os DFs gerados pela task anterior e produzir os DFs de análise
    run_analyses_task = PythonOperator(
        task_id='run_analyses_and_export',
        python_callable=task_run_analyses,
        # Não precisamos passar argumentos diretamente aqui
    )

    # Task 3: Carregamento Final para o BigQuery
    # Esta task vai ler os DFs de análise e carregá-los para o BigQuery
    load_to_bigquery_task = PythonOperator(
        task_id='load_processed_data_to_bigquery',
        python_callable=task_load_to_bigquery_final,
        # As variáveis de ambiente serão lidas dentro da função wrapper
        op_kwargs={
            'project_id': GCP_PROJECT_ID,
            'dataset_id': BQ_DATASET_ID
        }
    )

    # Definir as dependências das tarefas
    extract_and_merge_task >> run_analyses_task >> load_to_bigquery_task