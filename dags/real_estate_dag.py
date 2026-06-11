import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR)
DATA_DIR     = os.environ.get(
    key="HOST_DATA_DIR",
    default="/opt/airflow/data"
)

# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------

DOCKER_URL = "unix://var/run/docker.sock"

DATA_MOUNT = Mount(
    source=DATA_DIR,
    target="/app/data",
    type="bind",
)

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="real_estate_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
) as dag:

    scrape = DockerOperator(
        task_id="scraper",
        image="real-estate-data-pipeline-scraper",
        command="python src/ingestion/imobiliare_scraper.py",
        mounts=[DATA_MOUNT],
        docker_url=DOCKER_URL,
        auto_remove="success",
    )

    silver = DockerOperator(
        task_id="silver",
        image="real-estate-data-pipeline-transform",
        command="python src/pipeline/01_Bronze_to_Silver.py",
        mounts=[DATA_MOUNT],
        docker_url=DOCKER_URL,
        auto_remove="success",
    )

    gold = DockerOperator(
        task_id="gold",
        image="real-estate-data-pipeline-gold",
        command="python src/pipeline/02_Silver_to_Gold.py",
        mounts=[DATA_MOUNT],
        docker_url=DOCKER_URL,
        auto_remove="success",
    )

    scrape >> silver >> gold
