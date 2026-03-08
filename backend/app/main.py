from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import random
from typing import Annotated
import urllib.error
import urllib.request

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .kafka_producer import UserInteractionProducer
from .neo4j_client import Neo4jClient
from .redis_cache import RecommendationCache
from .schemas import (
    ProductOut,
    RecommendationOut,
    SimulateTrafficRequest,
    SimulateTrafficResponse,
    TriggerPipelineResponse,
    TrackEventRequest,
    TrackEventResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    neo4j = Neo4jClient(
        uri=settings.neo4j_uri,
        user=settings.neo4j_user,
        password=settings.neo4j_password,
    )
    producer = UserInteractionProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )
    recommendation_cache = RecommendationCache(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        key_prefix=settings.redis_recs_prefix,
        active_version_key=settings.redis_active_version_key,
    )

    app.state.neo4j = neo4j
    app.state.producer = producer
    app.state.recommendation_cache = recommendation_cache
    yield

    recommendation_cache.close()
    producer.close()
    neo4j.close()


app = FastAPI(title="Recommendation Backend", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/products", response_model=list[ProductOut])
def get_products(limit: Annotated[int, Query(ge=1, le=50)] = 12) -> list[ProductOut]:
    products = app.state.neo4j.get_random_products(limit=limit)
    return [ProductOut(**product) for product in products]


@app.post("/track", response_model=TrackEventResponse, status_code=202)
def track_interaction(payload: TrackEventRequest) -> TrackEventResponse:
    event = {
        "user_id": payload.user_id,
        "product_id": payload.product_id,
        "action": payload.action.value,
        "event_time": datetime.now(timezone.utc).isoformat(),
    }

    try:
        app.state.producer.publish(
            topic=settings.kafka_topic_user_interactions,
            key=payload.user_id,
            event=event,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to publish event: {exc}") from exc

    return TrackEventResponse(status="accepted", topic=settings.kafka_topic_user_interactions)


@app.get("/recommendations/{user_id}", response_model=list[RecommendationOut])
def get_recommendations(
    user_id: str, limit: Annotated[int, Query(ge=1, le=50)] = 10
) -> list[RecommendationOut]:
    cached = app.state.recommendation_cache.get_recommendations(user_id=user_id)
    if not cached:
        return []

    normalized: list[RecommendationOut] = []
    for item in cached:
        if "supporting_users" not in item and "supporting_signals" in item:
            item["supporting_users"] = item["supporting_signals"]
        try:
            normalized.append(RecommendationOut(**item))
        except Exception:
            continue

    return normalized[:limit]


def _build_simulation_events(payload: SimulateTrafficRequest) -> list[dict[str, str]]:
    rng = random.Random(payload.seed)
    users = [f"user_{i}" for i in range(payload.user_start, payload.user_end + 1)]
    products = [f"product_{i}" for i in range(payload.product_start, payload.product_end + 1)]

    events: list[dict[str, str]] = []
    for _ in range(payload.viewed_count):
        events.append(
            {
                "user_id": rng.choice(users),
                "product_id": rng.choice(products),
                "action": "VIEWED",
            }
        )
    for _ in range(payload.purchased_count):
        events.append(
            {
                "user_id": rng.choice(users),
                "product_id": rng.choice(products),
                "action": "PURCHASED",
            }
        )
    rng.shuffle(events)
    return events


@app.post("/admin/simulate-traffic", response_model=SimulateTrafficResponse, status_code=202)
def simulate_traffic(payload: SimulateTrafficRequest) -> SimulateTrafficResponse:
    if payload.user_start > payload.user_end:
        raise HTTPException(status_code=400, detail="user_start must be <= user_end")
    if payload.product_start > payload.product_end:
        raise HTTPException(status_code=400, detail="product_start must be <= product_end")

    events = _build_simulation_events(payload)
    timestamp = datetime.now(timezone.utc).isoformat()

    published = 0
    try:
        for event in events:
            app.state.producer.publish(
                topic=settings.kafka_topic_user_interactions,
                key=event["user_id"],
                event={
                    "user_id": event["user_id"],
                    "product_id": event["product_id"],
                    "action": event["action"],
                    "event_time": timestamp,
                    "synthetic": True,
                },
            )
            published += 1
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to publish synthetic traffic after {published} events: {exc}",
        ) from exc

    return SimulateTrafficResponse(
        status="accepted",
        topic=settings.kafka_topic_user_interactions,
        total_events=published,
        viewed_events=payload.viewed_count,
        purchased_events=payload.purchased_count,
    )


@app.post("/admin/trigger-ml-pipeline", response_model=TriggerPipelineResponse, status_code=202)
def trigger_ml_pipeline() -> TriggerPipelineResponse:
    dag_id = settings.airflow_recommendation_dag_id
    dag_run_id = f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    endpoint = f"{settings.airflow_api_base_url.rstrip('/')}/dags/{dag_id}/dagRuns"

    auth_token = base64.b64encode(
        f"{settings.airflow_username}:{settings.airflow_password}".encode("utf-8")
    ).decode("utf-8")
    request_body = {
        "dag_run_id": dag_run_id,
        "conf": {
            "triggered_by": "backend-admin-endpoint",
            "trigger_time_utc": datetime.now(timezone.utc).isoformat(),
        },
    }
    request = urllib.request.Request(
        url=endpoint,
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Basic {auth_token}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response_data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(
            status_code=503,
            detail=f"Airflow API error ({exc.code}). Check credentials/service health: {details}",
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to trigger Airflow DAG: {exc}") from exc

    return TriggerPipelineResponse(
        status="accepted",
        dag_id=dag_id,
        dag_run_id=response_data.get("dag_run_id", dag_run_id),
        state=response_data.get("state"),
        message="Airflow DAG run triggered",
    )
