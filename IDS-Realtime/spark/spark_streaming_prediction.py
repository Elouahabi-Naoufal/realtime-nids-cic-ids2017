import os
os.environ["KERAS_BACKEND"] = "torch"

import json
import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pymongo import MongoClient
import xgboost as xgb
import keras

from pyspark.sql import SparkSession


KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "network-flows"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "ids_db"
MONGO_COLLECTION = "predictions"

RF_MODEL_PATH   = "models/random_forest_multiclass_model.joblib"
XGB_MODEL_PATH  = "models/xgboost_multiclass_final.ubj"
DL_MODEL_PATH   = "models/cnn_bilstm_transformer_multiclass_v5.keras"

RF_ENCODER_PATH  = "models/label_encoder_rf.joblib"
XGB_ENCODER_PATH = "models/label_encoder_xgb.joblib"

RF_FEATURES_PATH  = "models/rf_features.json"
XGB_FEATURES_PATH = "models/xgb_features.json"

# DL model outputs 11 classes (attack-only, trained on CIC-IDS2017 subset)
# Index → attack name mapping (order from training)
DL_CLASS_NAMES = [
    "FTP-Patator", "SSH-Patator", "DoS Slowloris", "DoS Slowhttptest",
    "DoS Hulk", "DoS GoldenEye", "Heartbleed", "PortScan",
    "DDoS", "Bot", "Web Attack"
]

# =========================
# Load models (once at startup)
# =========================

print("Loading Random Forest...")
rf_model   = joblib.load(RF_MODEL_PATH)
rf_encoder = joblib.load(RF_ENCODER_PATH)

print("Loading XGBoost...")
xgb_model   = xgb.Booster()
xgb_model.load_model(XGB_MODEL_PATH)
xgb_encoder = joblib.load(XGB_ENCODER_PATH)

print("Loading DL multiclass (CNN-BiLSTM-Transformer)...")
dl_model = keras.saving.load_model(DL_MODEL_PATH, compile=False)

with open(RF_FEATURES_PATH)  as f: rf_features  = json.load(f)
with open(XGB_FEATURES_PATH) as f: xgb_features = json.load(f)

print(f"RF features: {len(rf_features)} | XGB features: {len(xgb_features)} | DL input: {dl_model.input_shape}")


# =========================
# Helpers
# =========================

def prepare_features(pdf, features):
    X = pdf.copy()
    for col in ["flow_id", "true_label", "event_time", "timestamp"]:
        if col in X.columns:
            X = X.drop(columns=[col])
    X.columns = X.columns.str.strip()
    for f in features:
        if f not in X.columns:
            X[f] = 0
    X = X[features]
    X = X.replace([np.inf, -np.inf], np.nan)
    X = X.apply(pd.to_numeric, errors="coerce").fillna(0)
    return X

def decode_rf(pred):
    try:
        return str(rf_encoder.inverse_transform([int(pred)])[0]).strip()
    except Exception:
        return str(pred)

def decode_xgb(pred_index):
    try:
        return str(xgb_encoder.inverse_transform([int(pred_index)])[0]).strip()
    except Exception:
        return str(pred_index)

def decode_dl(pred_index):
    if 0 <= pred_index < len(DL_CLASS_NAMES):
        return DL_CLASS_NAMES[pred_index]
    return f"class_{pred_index}"

def is_benign(label):
    s = str(label).strip().lower()
    return s in ("0", "benign", "normal")


# =========================
# Batch prediction
# =========================

def predict_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    rows = batch_df.select("value").toPandas()
    records = []
    for v in rows["value"]:
        try:
            records.append(json.loads(v))
        except Exception:
            pass
    if not records:
        return

    pdf = pd.DataFrame(records)
    flow_ids    = pdf.get("flow_id",    pd.Series(range(len(pdf))))
    true_labels = pdf.get("true_label", pd.Series(["unknown"] * len(pdf)))

    # RF prediction (batch)
    X_rf   = prepare_features(pdf, rf_features)
    rf_preds = rf_model.predict(X_rf)

    # DL prediction (batch) — reshape to (N, 44, 1)
    X_dl = prepare_features(pdf, rf_features).values.reshape(-1, len(rf_features), 1).astype("float32")
    dl_probs = dl_model.predict(X_dl, verbose=0)
    dl_preds = np.argmax(dl_probs, axis=1)

    final_results = []

    for idx in range(len(rf_preds)):
        rf_label = decode_rf(rf_preds[idx])
        dl_label = decode_dl(int(dl_preds[idx]))

        result = {
            "flow_id":       int(flow_ids.iloc[idx]),
            "timestamp":     datetime.now(timezone.utc).isoformat(),
            "true_label":    str(true_labels.iloc[idx]).strip(),
            "rf_prediction": rf_label,
            "dl_prediction": dl_label,
            "status":        None,
            "xgb_attack_type": None,
            "model_used":    None,
        }

        if is_benign(rf_label):
            result["status"]        = "NORMAL"
            result["xgb_attack_type"] = "None"
            result["model_used"]    = "Random Forest + DL"
        else:
            result["status"] = "ATTACK"

            # XGBoost (per-row)
            X_xgb   = prepare_features(pdf.iloc[[idx]], xgb_features)
            dmatrix = xgb.DMatrix(X_xgb, feature_names=xgb_features)
            xgb_out = xgb_model.predict(dmatrix)
            xgb_idx = int(np.argmax(xgb_out)) if xgb_out.ndim == 1 else int(np.argmax(xgb_out[0]))
            result["xgb_attack_type"] = decode_xgb(xgb_idx)
            result["model_used"]      = "Random Forest + XGBoost + DL"

        final_results.append(result)

    client     = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]
    collection.insert_many(final_results)
    print(f"Batch {batch_id}: {len(final_results)} predictions → MongoDB")


# =========================
# Spark Streaming
# =========================

spark = SparkSession.builder.appName("IDS-Realtime-Prediction").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

query = (
    kafka_df.selectExpr("CAST(value AS STRING) as value")
    .writeStream
    .foreachBatch(predict_batch)
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/ids_stream")
    .start()
)

print("Spark Streaming started. Waiting for Kafka messages...")
query.awaitTermination()
