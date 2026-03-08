from __future__ import annotations

import os


class Settings:
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "password")

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_user_interactions: str = os.getenv("KAFKA_TOPIC_USER_INTERACTIONS", "user-interactions")

    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    redis_recs_prefix: str = os.getenv("REDIS_RECS_PREFIX", "recs:")
    redis_active_version_key: str = os.getenv("REDIS_ACTIVE_VERSION_KEY", "recs:active_version")

    airflow_api_base_url: str = os.getenv("AIRFLOW_API_BASE_URL", "http://localhost:8080/api/v1")
    airflow_username: str = os.getenv("AIRFLOW_USERNAME", "airflow")
    airflow_password: str = os.getenv("AIRFLOW_PASSWORD", "airflow")
    airflow_recommendation_dag_id: str = os.getenv(
        "AIRFLOW_RECOMMENDATION_DAG_ID", "recommendation_gds_pipeline"
    )

    cors_origins: list[str] = os.getenv(
        "CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"
    ).split(",")


settings = Settings()
