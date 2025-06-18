# pipeline_cafe
Data Pipeline of a cafe dataset
# pipeline_cafe

## Overview

**pipeline_cafe** is a data pipeline project for analyzing a fictional cafe's dataset, including customers, events, and offers. The pipeline performs extraction, transformation, analysis, and loads processed data into Google BigQuery. It is built with Python, Pandas, and orchestrated using Apache Airflow.

## Project Structure

```
data_pipeline_cafe/
├── docker-compose.yaml
├── gcp-credentials/
│   └── data-pipeline-cafe-0f0423899328.json
└── pipeline_cafe/
    ├── README.md
    ├── dataset/
    │   ├── customers.csv
    │   ├── events.csv
    │   ├── offers.csv
    │   └── ...
    ├── output/
    │   ├── age_stats_df.parquet
    │   ├── df_avg_time.parquet
    │   └── effectiveness_channel.parquet
    └── pipeline/
        ├── workflow.py
        ├── workflow_dag.py
        ├── tasks_wrapper.py
        ├── requirements.txt
        └── dockerfile
```

- **dataset/**: Raw CSV data files.
- **output/**: Processed data exported as Parquet files.
- **pipeline/**: Main pipeline code, Airflow DAG, task wrappers, and dependencies.

## Main Features

- **Extraction** of customer, event, and offer data.
- **Transformation** and cleaning of datasets.
- **Analysis**:
  - Most effective marketing channel.
  - Age distribution of customers by offer completion status.
  - Average time to complete an offer.
- **Export** of results as Parquet files.
- **Load** processed data into Google BigQuery.
- **Orchestration** with Apache Airflow (DAG ready for use).

## How to Run

### 1. Prerequisites

- Docker and Docker Compose installed.
- Google Cloud BigQuery project and dataset.
- GCP service account key (JSON) in `gcp-credentials/`.

### 2. Environment Variables

Set in `docker-compose.yaml`:

- `GCP_PROJECT_ID`: Your GCP project ID.
- `BQ_DATASET_ID`: Your BigQuery dataset (e.g., `data-pipeline-cafe.data_set_cafe`).
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to the service account key inside the container.

### 3. Running Locally

Install dependencies:

```sh
cd data_pipeline_cafe/pipeline
pip install -r requirements.txt
```

Run the main pipeline:

```sh
python workflow.py
```

### 4. Running with Airflow + Docker

Start the services:

```sh
docker-compose up
```

Access Airflow at [http://localhost:8081](http://localhost:8081) and trigger the DAG `data_pipeline_cafe_original_workflow_dag`.

### 5. Results

Parquet files will be generated in `pipeline_cafe/output/` and loaded into your BigQuery tables.

## Notes

- File paths and environment variables are configured for both local and Docker/Airflow environments.
- Ensure your GCP credentials are correct and accessible by Airflow.

## License

This project is for educational purposes only.

---

> For questions or suggestions, please open an issue or contact the maintainer.
