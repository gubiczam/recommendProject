from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


COMMON_ENV = {
    "NEO4J_URI": "bolt://neo4j:7687",
    "NEO4J_USER": "neo4j",
    "NEO4J_PASSWORD": "password",
    "REDIS_HOST": "redis",
    "REDIS_PORT": "6379",
    "REDIS_DB": "0",
    "REDIS_RECS_PREFIX": "recs:",
    "REDIS_ACTIVE_VERSION_KEY": "recs:active_version",
    "RECOMMENDATION_LIMIT": "10",
    "WEIGHT_PERSONALIZED": "0.7",
    "WEIGHT_POPULARITY": "0.2",
    "WEIGHT_RECENCY": "0.1",
    "QUALITY_RECALL_AT_K_MIN": "0.05",
    "QUALITY_NDCG_AT_K_MIN": "0.03",
    "QUALITY_MAP_AT_K_MIN": "0.03",
    "QUALITY_COVERAGE_MIN": "0.5",
}


with DAG(
    dag_id="recommendation_gds_pipeline",
    description="Train, evaluate, and publish graph recommendations",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 3, 8, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mlops",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["recommendations", "gds", "redis", "mlops"],
) as dag:
    train = BashOperator(
        task_id="train_recommender",
        bash_command="python /opt/airflow/mlops/gds_pipeline.py --stage train",
        env=COMMON_ENV,
    )

    evaluate = BashOperator(
        task_id="evaluate_recommender",
        bash_command="python /opt/airflow/mlops/gds_pipeline.py --stage evaluate",
        env=COMMON_ENV,
    )

    publish = BashOperator(
        task_id="publish_recommendations",
        bash_command="python /opt/airflow/mlops/gds_pipeline.py --stage publish",
        env=COMMON_ENV,
    )

    train >> evaluate >> publish
