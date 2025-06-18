import pandas as pd
from pathlib import Path
import logging
import os
from google.cloud import bigquery # Importar aqui para que o cliente possa ser inicializado

# Importar as funções e variáveis globais do seu workflow.py original
# CUIDADO: Isso traz as variáveis globais para este escopo.
# É uma forma de não alterar workflow.py, mas tem suas limitações e riscos em produção.
from workflow import (
    extract_offer_id,
    merge_dfs,
    first_question,
    second_question,
    last_question,
    export_to_parquet,
    load_to_bigquery,
    # Importar as variáveis globais do seu workflow.py
    PROJECT_ID,
    DATASET_ID,
    treated_data_dir, # treated_data_dir = base_project_dir / "output"
    parquet_files_to_load # dict que já existe no seu workflow.py
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminhos absolutos para o ambiente Docker do Airflow
# Estes precisam ser consistentes com seus volumes no docker-compose.yaml
AIRFLOW_DATA_DIR = Path("/opt/airflow/output") # O mesmo que treated_data_dir dentro do container


def task_full_extraction_and_merge(**kwargs):
    """
    Task para extrair, transformar e mesclar os DataFrames iniciais.
    Salva dfs_total e df_customer em arquivos Parquet para a próxima task.
    """
    logging.info("Iniciando task_full_extraction_and_merge...")
    df_event, df_customer, df_offer = extract_offer_id() # Sua função original
    dfs_total = merge_dfs(df_event, df_customer, df_offer) # Sua função original

    # Salva os DataFrames para que as próximas tasks possam lê-los
    dfs_total.to_parquet(AIRFLOW_DATA_DIR / "dfs_total.parquet", index=False)
    df_customer.to_parquet(AIRFLOW_DATA_DIR / "df_customer.parquet", index=False)
    logging.info("dfs_total e df_customer salvos em Parquet.")


def task_run_analyses(**kwargs):
    """
    Task para executar as análises da primeira, segunda e última pergunta.
    Salva os resultados em arquivos Parquet individuais.
    """
    logging.info("Iniciando task_run_analyses...")
    # Carrega os DataFrames da task anterior
    dfs_total = pd.read_parquet(AIRFLOW_DATA_DIR / "dfs_total.parquet")
    df_customer = pd.read_parquet(AIRFLOW_DATA_DIR / "df_customer.parquet")

    # Suas funções originais
    effectiveness_channel = first_question(dfs_total) # removemos most_effective, high_value_conclusion_rate
    age_stats_df = second_question(dfs_total, df_customer) # REMOVIDO plt.show() do workflow.py para esta task funcionar
    avg_time_hours, avg_time_days = last_question(dfs_total)

    # Prepara df_avg_time (parte do seu main original)
    df_avg_time = pd.DataFrame({
        'metric': ['avg_time_hours', 'avg_time_days'],
        'value': [avg_time_hours, avg_time_days]
    })
    df_avg_time['value'] = df_avg_time['value'].round(2)

    _effectiveness_channel_df, _most_effective, _high_value_conclusion_rate = first_question(dfs_total)
    
    export_to_parquet(
        _effectiveness_channel_df, # effectiveness_channel
        _most_effective,           # most_effective
        _high_value_conclusion_rate,# high_value_conclusion_rate
        age_stats_df,              # age_stats_df
        df_avg_time,               # df_avg_time
        str(AIRFLOW_DATA_DIR)      # folder_path (como string)
    )
    logging.info("Resultados das análises exportados para Parquet.")


def task_load_to_bigquery_final(**kwargs):
    """
    Task para carregar os arquivos Parquet resultantes para o BigQuery.
    """
    logging.info("Iniciando task_load_to_bigquery_final...")
    # O `client` e `parquet_files_to_load` são globais no seu workflow.py.
    # Precisamos garantir que eles sejam acessíveis ou recriados aqui.
    # Para o `client`, é mais seguro recriá-lo na task.

    # Recriar o cliente BigQuery dentro da task
    _project_id = os.getenv("GCP_PROJECT_ID", PROJECT_ID) # Usa a global se a env var não estiver setada
    _dataset_id = os.getenv("BQ_DATASET_ID", DATASET_ID) # Usa a global se a env var não estiver setada
    client_bq_task = bigquery.Client(project=_project_id)
    logging.info(f"BigQuery Client inicializado para PROJECT_ID: {_project_id}")

    # Assegurar que parquet_files_to_load usa o path correto dentro do container
    # O seu `workflow.py` já define `treated_data_dir = base_project_dir / "output"`
    # e `parquet_files_to_load` usa `treated_data_dir`.
    # Vamos garantir que os paths nos dicionário são os do contêiner.
    _parquet_files_to_load = {
        "df_avg_time_table": AIRFLOW_DATA_DIR / "df_avg_time.parquet",
        "effectiveness_table": AIRFLOW_DATA_DIR / "effectiveness_channel.parquet",
        "age_stats_table": AIRFLOW_DATA_DIR / "age_stats_df.parquet"
    }

    logging.info(f"Carregando dados para o dataset '{_dataset_id}' no BigQuery...")
    for table_name, file_path in _parquet_files_to_load.items():
        load_to_bigquery(file_path, table_name, client_bq_task, _dataset_id, _project_id) # Sua função original
    logging.info("Processo de carregamento de dados para BigQuery concluído.")