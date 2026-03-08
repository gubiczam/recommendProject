from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import redis
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_RECS_PREFIX = os.getenv("REDIS_RECS_PREFIX", "recs:")
REDIS_ACTIVE_VERSION_KEY = os.getenv("REDIS_ACTIVE_VERSION_KEY", "recs:active_version")
REDIS_METRICS_PREFIX = os.getenv("REDIS_METRICS_PREFIX", "recs:metrics:")
REDIS_METRICS_LATEST_KEY = os.getenv("REDIS_METRICS_LATEST_KEY", "recs:metrics:latest")

GRAPH_NAME = os.getenv("GDS_GRAPH_NAME", "reco-graph")
EMBEDDING_PROPERTY = os.getenv("GDS_EMBEDDING_PROPERTY", "fastrpEmbedding")
EMBEDDING_DIMENSION = int(os.getenv("GDS_EMBEDDING_DIMENSION", "128"))
USER_KNN_TOP_K = int(os.getenv("USER_KNN_TOP_K", "5"))
KNN_SIMILARITY_CUTOFF = float(os.getenv("KNN_SIMILARITY_CUTOFF", "0.0"))
RECOMMENDATION_LIMIT = int(os.getenv("RECOMMENDATION_LIMIT", "10"))
TRAIN_VIEWED_REL = "TRAIN_VIEWED"
TRAIN_PURCHASED_REL = "TRAIN_PURCHASED"

WEIGHT_PERSONALIZED = float(os.getenv("WEIGHT_PERSONALIZED", "0.7"))
WEIGHT_POPULARITY = float(os.getenv("WEIGHT_POPULARITY", "0.2"))
WEIGHT_RECENCY = float(os.getenv("WEIGHT_RECENCY", "0.1"))
RECENCY_HALF_LIFE_DAYS = float(os.getenv("RECENCY_HALF_LIFE_DAYS", "30"))

QUALITY_RECALL_AT_K_MIN = float(os.getenv("QUALITY_RECALL_AT_K_MIN", "0.05"))
QUALITY_NDCG_AT_K_MIN = float(os.getenv("QUALITY_NDCG_AT_K_MIN", "0.03"))
QUALITY_MAP_AT_K_MIN = float(os.getenv("QUALITY_MAP_AT_K_MIN", "0.03"))
QUALITY_COVERAGE_MIN = float(os.getenv("QUALITY_COVERAGE_MIN", "0.5"))

ARTIFACT_DIR = Path(os.getenv("ARTIFACT_DIR", str(Path(__file__).parent / "artifacts")))
CANDIDATES_PATH = ARTIFACT_DIR / "candidates.json"
METRICS_PATH = ARTIFACT_DIR / "metrics.json"
DECISION_PATH = ARTIFACT_DIR / "decision.json"


def _ensure_artifact_dir() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_artifact_dir()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Artifact not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_gds_available(tx: Any) -> str:
    try:
        row = tx.run("RETURN gds.version() AS version").single()
    except Neo4jError:
        row = tx.run("CALL gds.version() YIELD version RETURN version").single()
    if not row or not row["version"]:
        raise RuntimeError("GDS library is not available in Neo4j.")
    return str(row["version"])


def drop_existing_projection_if_needed(tx: Any, graph_name: str) -> None:
    exists_row = tx.run(
        "CALL gds.graph.exists($graph_name) YIELD exists RETURN exists",
        graph_name=graph_name,
    ).single()
    exists = bool(exists_row and exists_row["exists"])
    if exists:
        tx.run(
            "CALL gds.graph.drop($graph_name, false) YIELD graphName RETURN graphName",
            graph_name=graph_name,
        ).consume()


def build_projection(tx: Any, graph_name: str) -> None:
    tx.run(
        """
        CALL gds.graph.project(
          $graph_name,
          ['User', 'Product'],
          [$train_viewed_rel, $train_purchased_rel]
        )
        """,
        graph_name=graph_name,
        train_viewed_rel=TRAIN_VIEWED_REL,
        train_purchased_rel=TRAIN_PURCHASED_REL,
    ).consume()


def run_fastrp(tx: Any, graph_name: str) -> None:
    tx.run(
        """
        CALL gds.fastRP.mutate(
          $graph_name,
          {
            mutateProperty: $embedding_property,
            embeddingDimension: $embedding_dimension,
            randomSeed: 42,
            concurrency: 1
          }
        )
        """,
        graph_name=graph_name,
        embedding_property=EMBEDDING_PROPERTY,
        embedding_dimension=EMBEDDING_DIMENSION,
    ).consume()


def build_user_similarity_knn(tx: Any, graph_name: str) -> dict[str, Any]:
    tx.run("MATCH ()-[s:SIMILAR_USER]->() DELETE s").consume()
    result = tx.run(
        """
        CALL gds.knn.write(
          $graph_name,
          {
            nodeLabels: ['User'],
            nodeProperties: [$embedding_property],
            topK: $user_top_k,
            similarityCutoff: $similarity_cutoff,
            randomSeed: 42,
            concurrency: 1,
            writeRelationshipType: 'SIMILAR_USER',
            writeProperty: 'score'
          }
        )
        YIELD nodesCompared, relationshipsWritten
        RETURN nodesCompared, relationshipsWritten
        """,
        graph_name=graph_name,
        embedding_property=EMBEDDING_PROPERTY,
        user_top_k=USER_KNN_TOP_K,
        similarity_cutoff=KNN_SIMILARITY_CUTOFF,
    ).single()
    if not result:
        return {"nodesCompared": 0, "relationshipsWritten": 0}
    return {
        "nodesCompared": int(result["nodesCompared"]),
        "relationshipsWritten": int(result["relationshipsWritten"]),
    }


def cleanup_training_relationships(tx: Any) -> None:
    tx.run(
        f"MATCH ()-[r:{TRAIN_VIEWED_REL}|{TRAIN_PURCHASED_REL}]->() DELETE r"
    ).consume()


def materialize_training_relationships(
    tx: Any,
    holdout_by_user: dict[str, str | None],
) -> dict[str, int]:
    result = tx.run(
        """
        MATCH (u:User)-[r:VIEWED|PURCHASED]->(p:Product)
        WHERE $holdout_by_user[u.id] IS NULL OR p.id <> $holdout_by_user[u.id]
        WITH u, p, r,
             CASE type(r)
               WHEN 'VIEWED' THEN $train_viewed_rel
               WHEN 'PURCHASED' THEN $train_purchased_rel
             END AS train_rel_type
        CALL apoc.create.relationship(
          u,
          train_rel_type,
          {
            first_event_at: r.first_event_at,
            last_event_at: r.last_event_at,
            count: coalesce(r.count, 1)
          },
          p
        ) YIELD rel
        RETURN count(rel) AS relationships_created
        """,
        holdout_by_user=holdout_by_user,
        train_viewed_rel=TRAIN_VIEWED_REL,
        train_purchased_rel=TRAIN_PURCHASED_REL,
    ).single()
    relationships_created = int(result["relationships_created"]) if result else 0
    users_with_holdout = sum(1 for product_id in holdout_by_user.values() if product_id)
    return {
        "relationships_created": relationships_created,
        "users_with_holdout": users_with_holdout,
    }


def fetch_holdout_map(tx: Any) -> dict[str, str | None]:
    rows = tx.run(
        """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[r:VIEWED|PURCHASED]->(p:Product)
        WITH u, p, coalesce(r.last_event_at, r.first_event_at) AS ts
        ORDER BY u.id, ts DESC
        WITH u, collect(p.id)[0] AS holdout_product_id
        RETURN u.id AS user_id, holdout_product_id
        """
    )
    return {row["user_id"]: row["holdout_product_id"] for row in rows}


def compute_recommendations(
    tx: Any,
    holdout_by_user: dict[str, str | None],
    limit: int,
) -> list[dict[str, Any]]:
    rows = tx.run(
        """
        MATCH (u:User)
        WITH u, $holdout_by_user[u.id] AS holdout_product_id
        CALL (u, holdout_product_id) {
          OPTIONAL MATCH (u)-[su:SIMILAR_USER]->(peer:User)-[r:TRAIN_VIEWED|TRAIN_PURCHASED]->(candidate:Product)
          WHERE peer <> u
            AND (
              candidate.id = holdout_product_id OR
              NOT (u)-[:TRAIN_VIEWED|TRAIN_PURCHASED]->(candidate)
            )
          WITH candidate,
               sum((coalesce(su.score, 0.0) + 0.05) * CASE type(r) WHEN 'TRAIN_PURCHASED' THEN 3.0 ELSE 1.0 END) AS personalized_raw,
               count(DISTINCT peer) AS supporting_signals
          WHERE candidate IS NOT NULL
          OPTIONAL MATCH (candidate)<-[all_r:TRAIN_VIEWED|TRAIN_PURCHASED]-(:User)
          WITH candidate,
               supporting_signals,
               personalized_raw,
               count(all_r) AS popularity_count,
               max(coalesce(all_r.last_event_at, all_r.first_event_at)) AS latest_event_at
          WITH candidate,
               supporting_signals,
               (personalized_raw / (personalized_raw + 1.0)) AS personalized_score,
               log10(1.0 + toFloat(popularity_count)) AS popularity_score,
               CASE
                 WHEN latest_event_at IS NULL THEN 0.0
                 ELSE exp(-1.0 * duration.inDays(latest_event_at, datetime()).days / $recency_half_life_days)
               END AS recency_score
          WITH candidate,
               supporting_signals,
               ($w_personalized * personalized_score) +
               ($w_popularity * popularity_score) +
               ($w_recency * recency_score) AS hybrid_score
          ORDER BY hybrid_score DESC, supporting_signals DESC
          RETURN collect({
            id: candidate.id,
            name: candidate.name,
            category: candidate.category,
            price: candidate.price,
            score: round(hybrid_score * 1000.0) / 1000.0,
            supporting_signals: supporting_signals
          })[0..$limit] AS recs
        }
        RETURN u.id AS user_id, holdout_product_id, recs AS recommendations
        ORDER BY user_id
        """,
        holdout_by_user=holdout_by_user,
        recency_half_life_days=RECENCY_HALF_LIFE_DAYS,
        w_personalized=WEIGHT_PERSONALIZED,
        w_popularity=WEIGHT_POPULARITY,
        w_recency=WEIGHT_RECENCY,
        limit=limit,
    )
    return [dict(row) for row in rows]


def evaluate_candidates(rows: list[dict[str, Any]], k: int) -> dict[str, Any]:
    eligible_users = 0
    non_empty_users = 0
    hit_sum = 0.0
    ndcg_sum = 0.0
    map_sum = 0.0

    for row in rows:
        holdout = row.get("holdout_product_id")
        recs = row.get("recommendations", [])
        rec_ids = [r["id"] for r in recs[:k] if isinstance(r, dict) and "id" in r]

        if rec_ids:
            non_empty_users += 1

        if not holdout:
            continue

        eligible_users += 1
        if holdout in rec_ids:
            rank = rec_ids.index(holdout) + 1
            hit_sum += 1.0
            ndcg_sum += 1.0 / math.log2(rank + 1)
            map_sum += 1.0 / rank

    recall_at_k = (hit_sum / eligible_users) if eligible_users else 0.0
    ndcg_at_k = (ndcg_sum / eligible_users) if eligible_users else 0.0
    map_at_k = (map_sum / eligible_users) if eligible_users else 0.0
    coverage = (non_empty_users / len(rows)) if rows else 0.0

    return {
        "k": k,
        "eligible_users": eligible_users,
        "scored_users": len(rows),
        "coverage": round(coverage, 4),
        "recall_at_k": round(recall_at_k, 4),
        "ndcg_at_k": round(ndcg_at_k, 4),
        "map_at_k": round(map_at_k, 4),
    }


def quality_gate(metrics: dict[str, Any]) -> tuple[bool, list[str]]:
    checks = [
        (metrics["recall_at_k"] >= QUALITY_RECALL_AT_K_MIN, f"recall_at_k >= {QUALITY_RECALL_AT_K_MIN}"),
        (metrics["ndcg_at_k"] >= QUALITY_NDCG_AT_K_MIN, f"ndcg_at_k >= {QUALITY_NDCG_AT_K_MIN}"),
        (metrics["map_at_k"] >= QUALITY_MAP_AT_K_MIN, f"map_at_k >= {QUALITY_MAP_AT_K_MIN}"),
        (metrics["coverage"] >= QUALITY_COVERAGE_MIN, f"coverage >= {QUALITY_COVERAGE_MIN}"),
    ]
    failed = [desc for ok, desc in checks if not ok]
    return (len(failed) == 0, failed)


def run_train() -> dict[str, Any]:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            gds_version = session.execute_read(ensure_gds_available)
            print(f"GDS version: {gds_version}")

            holdout_by_user = session.execute_read(fetch_holdout_map)
            session.execute_write(cleanup_training_relationships)
            training_stats = session.execute_write(
                materialize_training_relationships,
                holdout_by_user,
            )

            session.execute_write(drop_existing_projection_if_needed, GRAPH_NAME)
            session.execute_write(build_projection, GRAPH_NAME)
            session.execute_write(run_fastrp, GRAPH_NAME)
            knn_stats = session.execute_write(build_user_similarity_knn, GRAPH_NAME)
            print(f"KNN write stats: {knn_stats}")
            rows = session.execute_read(compute_recommendations, holdout_by_user, RECOMMENDATION_LIMIT)

            session.execute_write(drop_existing_projection_if_needed, GRAPH_NAME)
            session.execute_write(cleanup_training_relationships)

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "gds_graph": GRAPH_NAME,
            "k": RECOMMENDATION_LIMIT,
            "holdout_strategy": "latest interacted product per user excluded from training graph",
            "training_stats": training_stats,
            "weights": {
                "personalized": WEIGHT_PERSONALIZED,
                "popularity": WEIGHT_POPULARITY,
                "recency": WEIGHT_RECENCY,
            },
            "rows": rows,
        }
        _write_json(CANDIDATES_PATH, payload)
        print(f"Saved candidate artifact: {CANDIDATES_PATH}")
        return payload
    finally:
        try:
            with driver.session() as session:
                session.execute_write(drop_existing_projection_if_needed, GRAPH_NAME)
                session.execute_write(cleanup_training_relationships)
        except Exception:
            pass
        finally:
            driver.close()


def run_evaluate() -> dict[str, Any]:
    candidates = _read_json(CANDIDATES_PATH)
    rows = candidates.get("rows", [])
    metrics = evaluate_candidates(rows, RECOMMENDATION_LIMIT)
    passed, failed_rules = quality_gate(metrics)

    decision = {
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "quality_passed": passed,
        "failed_rules": failed_rules,
        "metrics": metrics,
    }
    _write_json(METRICS_PATH, metrics)
    _write_json(DECISION_PATH, decision)
    print(f"Evaluation metrics: {metrics}")
    print(f"Quality gate passed: {passed}")
    if failed_rules:
        print(f"Failed rules: {failed_rules}")
    return decision


def publish_to_redis(rows: list[dict[str, Any]], decision: dict[str, Any]) -> None:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    try:
        version = f"v{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        quality_passed = bool(decision.get("quality_passed"))

        metrics_payload = {
            "version": version,
            "quality_passed": quality_passed,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "metrics": decision.get("metrics", {}),
            "failed_rules": decision.get("failed_rules", []),
        }

        pipe = redis_client.pipeline()

        pipe.set(f"{REDIS_METRICS_PREFIX}{version}", json.dumps(metrics_payload))
        pipe.set(REDIS_METRICS_LATEST_KEY, json.dumps(metrics_payload))

        if quality_passed:
            for row in rows:
                key = f"{REDIS_RECS_PREFIX}{version}:{row['user_id']}"
                pipe.set(key, json.dumps(row.get("recommendations", [])))
            pipe.set(REDIS_ACTIVE_VERSION_KEY, version)

        pipe.execute()

        if quality_passed:
            print(f"Published version {version} and switched active pointer.")
        else:
            print("Quality gate failed. Active version was NOT switched.")
    finally:
        redis_client.close()


def run_publish() -> dict[str, Any]:
    candidates = _read_json(CANDIDATES_PATH)
    decision = _read_json(DECISION_PATH)
    rows = candidates.get("rows", [])
    publish_to_redis(rows, decision)
    return decision


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run staged GDS recommendation pipeline.")
    parser.add_argument(
        "--stage",
        choices=["train", "evaluate", "publish", "all"],
        default="all",
        help="Pipeline stage to run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.stage == "train":
        run_train()
        return

    if args.stage == "evaluate":
        run_evaluate()
        return

    if args.stage == "publish":
        run_publish()
        return

    run_train()
    run_evaluate()
    run_publish()


if __name__ == "__main__":
    main()
