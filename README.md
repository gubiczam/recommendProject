# Real-Time Graph Recommendation Engine (Enterprise V2)

Production-style demo platform for graph-based recommendations with streaming ingestion, graph ML training, cache serving, orchestration, and a modern React admin/webshop UI.

## Stack
- Frontend: React + TypeScript + Vite + TailwindCSS
- API: FastAPI (Python)
- Stream ingestion: Kafka producer/consumer
- Online graph state: Neo4j
- Graph ML pipeline: Neo4j GDS (FastRP + KNN + hybrid ranking)
- Orchestration: Apache Airflow
- Recommendation serving cache: Redis
- Infra orchestration: Docker Compose

## Architecture
```text
React UI
  -> FastAPI /track
    -> Kafka topic user-interactions
      -> stream_processor
        -> Neo4j (VIEWED/PURCHASED graph)

Airflow DAG (recommendation_gds_pipeline)
  -> mlops/gds_pipeline.py --stage train
  -> mlops/gds_pipeline.py --stage evaluate
  -> mlops/gds_pipeline.py --stage publish
    -> Redis versioned recommendation keys

React UI
  -> FastAPI /recommendations/{user_id}
    -> Redis (active recommendation version)
```

## Repository layout
```text
airflow/            Airflow image + DAGs
backend/            FastAPI service
data_seeder/        Initial mock data seeding script
frontend/           React app (Webshop + Admin Dashboard)
mlops/              GDS pipeline and artifacts
stream_processor/   Kafka consumer writing interactions to Neo4j
docker-compose.yml  Full local infrastructure
simulate_traffic.py Synthetic traffic generator
```

## Prerequisites
- Docker Desktop (or Docker Engine + Compose)
- Python 3.11+
- Node.js 20+
- npm 10+

## 0 -> Running from scratch
Use 5 terminals.

### 1) Infrastructure
```bash
cd /Users/gubiczam/Desktop/recommendProject
docker-compose up -d --build
docker ps
```

### 2) Seed Neo4j data (first run or after reset)
```bash
cd /Users/gubiczam/Desktop/recommendProject/data_seeder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python seed.py
```

### 3) Start backend
```bash
cd /Users/gubiczam/Desktop/recommendProject/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 4) Start stream processor
```bash
cd /Users/gubiczam/Desktop/recommendProject/stream_processor
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

### 5) Start frontend
```bash
cd /Users/gubiczam/Desktop/recommendProject/frontend
npm install
npm run dev -- --host 0.0.0.0 --port 5173
```

Open:
- Frontend: http://localhost:5173
- Airflow: http://localhost:8080 (airflow / airflow)
- Neo4j Browser: http://localhost:7474 (neo4j / password)

## Core API
### Health
```bash
curl http://localhost:8000/health
```

### Product feed
```bash
curl http://localhost:8000/products
```

### Track interaction
```bash
curl -X POST http://localhost:8000/track \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user_1","product_id":"product_1","action":"VIEWED"}'
```

### Recommendations (served from Redis)
```bash
curl http://localhost:8000/recommendations/user_1
```

### Generate synthetic traffic via backend admin API
```bash
curl -X POST http://localhost:8000/admin/simulate-traffic \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Trigger GDS pipeline via backend admin API
```bash
curl -X POST http://localhost:8000/admin/trigger-ml-pipeline
```

## Airflow DAG
DAG id: `recommendation_gds_pipeline`

Stages:
1. `train_recommender`
2. `evaluate_recommender`
3. `publish_recommendations`

Manual API check:
```bash
curl -u airflow:airflow \
  "http://localhost:8080/api/v1/dags/recommendation_gds_pipeline/dagRuns?limit=5"
```

## Frontend capabilities
- Webshop route:
  - active user selector (user_1 ... user_10)
  - recommendation cards with score and support count
  - product actions: `View Details` -> `VIEWED`, `Buy Now` -> `PURCHASED`
- Admin Dashboard route:
  - links to Airflow and Neo4j
  - architecture view
  - `Generate Random Traffic` button
  - `Run GDS Pipeline Now` button

## Quality workflow
1. Generate interactions (`/track` or traffic simulator)
2. Verify stream processor consumes events
3. Trigger pipeline (`Run GDS Pipeline Now`)
4. Read recommendations from API (`/recommendations/{user_id}`)

## Troubleshooting
### `curl: (7) Failed to connect to localhost:8000`
Backend is not running. Start uvicorn in `backend/`.

### Recommendations are empty
- Stream processor may not be running
- GDS pipeline may not have published a successful version
- Check Redis key:
```bash
docker exec redis redis-cli GET recs:user_1
docker exec redis redis-cli GET recs:active_version
```

### Airflow trigger fails from backend
- Verify Airflow webserver is healthy at `http://localhost:8080/health`
- Verify backend env values:
  - `AIRFLOW_API_BASE_URL`
  - `AIRFLOW_USERNAME`
  - `AIRFLOW_PASSWORD`

## Security note
This repository uses local development credentials and plaintext local networking. Do not reuse these settings in production.
