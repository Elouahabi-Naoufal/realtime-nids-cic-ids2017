# IDS-Realtime — Pipeline Architecture

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                              │
│                    data/X_test.csv (44 features)                │
│                    data/y_test.csv (true labels)                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     KAFKA PRODUCER                              │
│          producer/kafka_producer.py                             │
│     Sends each row as JSON → topic: network-flows               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     KAFKA (Docker)                              │
│                  topic: network-flows                           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              SPARK STRUCTURED STREAMING                         │
│         spark/spark_streaming_prediction.py                     │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │            RANDOM FOREST  (44 features)                 │   │
│   │         random_forest_multiclass_model.joblib           │   │
│   └──────────────┬──────────────────────┬───────────────────┘   │
│                  │                      │                        │
│            BENIGN (0)             ATTACK (1–14)                  │
│                  │                      │                        │
│                  ▼         ┌────────────┴────────────┐          │
│            status=NORMAL   │                         │          │
│            dl=None         ▼                         ▼          │
│            xgb=None  ┌──────────────┐   ┌──────────────────┐   │
│                      │   XGBOOST    │   │  CNN-BiLSTM      │   │
│                      │  (44 feat.)  │   │  Transformer     │   │
│                      │  attack type │   │  (44 feat.)      │   │
│                      │  labels 1–14 │   │  11 classes      │   │
│                      └──────┬───────┘   └───────┬──────────┘   │
│                             │                   │               │
│                      xgb_attack_type      dl_prediction         │
│                                   │       │                     │
│                            status = ATTACK                      │
│                        model = RF + XGBoost + DL                │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        MONGODB                                  │
│                   ids_db.predictions                            │
│                                                                 │
│  {                                                              │
│    flow_id, timestamp, true_label,                              │
│    rf_prediction, xgb_attack_type,                              │
│    dl_prediction, status, model_used                            │
│  }                                                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  STREAMLIT DASHBOARD                            │
│                    ui/dashboard.py                              │
│                                                                 │
│  · KPI cards (total, normal, attacks, attack types)             │
│  · Traffic timeline (area chart per minute)                     │
│  · XGBoost attack distribution (bar chart)                      │
│  · Deep Learning attack distribution (bar chart)                │
│  · Live predictions table (searchable, filterable)              │
│                                                                 │
│                  http://localhost:8501                          │
└─────────────────────────────────────────────────────────────────┘
```

## Models

| Model | Input | Output | Trigger |
|---|---|---|---|
| Random Forest | 44 features | 15 classes (0=BENIGN, 1–14=attacks) | Every flow |
| XGBoost | 44 features | 14 attack classes (1–14) | RF says ATTACK |
| CNN-BiLSTM-Transformer | (44, 1) sequence | 11 attack classes | RF says ATTACK |

## Stored document example

```json
{
  "flow_id": 42,
  "timestamp": "2026-06-20T15:38:49+00:00",
  "true_label": "DDoS",
  "rf_prediction": "2",
  "xgb_attack_type": "2",
  "dl_prediction": "DDoS",
  "status": "ATTACK",
  "model_used": "Random Forest + XGBoost + DL"
}
```

## Launch order

```bash
# 1. Infrastructure
docker compose up -d

# 2. Spark (Terminal 1)
KERAS_BACKEND=torch PYSPARK_PYTHON=venv/bin/python \
  venv/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
  spark/spark_streaming_prediction.py

# 3. Dashboard (Terminal 2)
venv/bin/streamlit run ui/dashboard.py --server.address 0.0.0.0

# 4. Producer (Terminal 3 — after Spark is ready)
venv/bin/python producer/kafka_producer.py --limit 100 --sleep 0.5
```
