from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_PATH = "/opt/airflow/dags/dbt-project"
DBT_PROFILES_PATH = "/opt/airflow/dags/dbt-project/profiles.yml"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 10),
    'retries': 1,
}

with DAG(
    dag_id='dbt_transform',
    default_args=default_args,
    description='Transform raw data using dbt with Cosmos',
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_dag = DbtTaskGroup(
        dag=dag,
        group_id='dbt_transform',
        default_args=default_args,
        operator_args={
            "install_deps": True,
            "full_refresh": True
        },
        project_config=ProjectConfig(
            dbt_project_path=str(DBT_PROJECT_PATH),
            models_relative_path="models",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=str(DBT_EXECUTABLE_PATH),
        ),
        profile_config=ProfileConfig(
            profile_name="ecommerce",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_ecommerce",
                profile_args={"schema": "public"}
            )
        )
    )