from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "alaa",
    "start_date": datetime(2026, 5, 6),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_pipeline",
    default_args=default_args,
    schedule="0 6 * * *",
    catchup=False,
    description="Fetch data from APIs and send to Kafka"
) as dag:

    github_task = BashOperator(
        task_id="github_producer",
        bash_command="bash -c 'python /opt/airflow/producers/github_trends.py'"
    )

    jobs_task = BashOperator(
        task_id="jobs_producer",
        bash_command="bash -c 'python /opt/airflow/producers/jobs_producer.py'"
    )

    news_task = BashOperator(
        task_id="news_producer",
        bash_command="bash -c 'python /opt/airflow/producers/news_producers.py'"
    )

    exchange_task = BashOperator(
        task_id="exchange_rate_producer",
        bash_command="bash -c 'python /opt/airflow/producers/exchange_rate.py'"
    )

    [github_task, jobs_task, news_task, exchange_task]