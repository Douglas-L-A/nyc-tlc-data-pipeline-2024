from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG - Data Pipeline
default_args = {
    'owner': 'Douglas',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='nyc_tlc_pipeline',
    default_args=default_args,
    description='Pipeline de dados NYC TLC - Bronze → Silver → Gold',
    schedule_interval=None,      
    catchup=False,
    tags=['nyc_tlc', 'aws', 'dbt', 'bronze-silver-gold']
) as dag:
    # Extract - Camada Bronze
    extract_bronze = BashOperator(
        task_id='extract_bronze',
        bash_command='python3 /opt/airflow/scripts/extract_bronze.py'
    )

    # Transform - Camada Silver
    transform_silver = BashOperator(
        task_id='transform_silver',
        bash_command='python3 /opt/airflow/scripts/transform_silver.py'
    )

    #Analytics (DBT) - Camada Gold
    run_dbt_gold = BashOperator(
        task_id='run_dbt_gold',
        bash_command='cd /opt/airflow/dbt && dbt run --models gold'
    )

    extract_bronze >> transform_silver >> run_dbt_gold
