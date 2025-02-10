from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig
from cosmos.config import ProfileConfig, RenderConfig
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 10),
    'retries': 1,
}

dag = DAG(
    'dbt_transform',
    default_args=default_args,
    description='Transform raw data using dbt with Cosmos',
    schedule_interval=None,
    catchup=False
)

dbt_project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dags/dbt-project",
)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt-project/profiles.yml"
)

with dag:
    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=dbt_project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:models"]
        )
    ) 