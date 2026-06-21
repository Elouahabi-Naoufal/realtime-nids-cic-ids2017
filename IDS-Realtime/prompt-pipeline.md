# CONTEXT

I have an existing real-time network intrusion detection pipeline:

```
raw .pcap
      ↓
producer (currently kafka_producer.py reading X_test.csv)
      ↓
Kafka topic "network-flows"
      ↓
spark_streaming_prediction.py
      ↓
Random Forest + XGBoost + Deep Learning models
      ↓
MongoDB (ids_db.predictions)
      ↓
dashboard.py (Streamlit, http://localhost:8501)
```

Everything from **Kafka onward already works** and **must NOT be modified**.

The only component you are building/replacing is the producer stage.

Currently, `producer/kafka_producer.py` reads already preprocessed rows from `data/X_test.csv` and publishes them to Kafka.

I want to replace **only** this CSV-reading logic with **real-time flow extraction directly from raw CICIDS2017 `.pcap` files**, using **NFStream**.

The desired architecture is:

```
Raw packets
      ↓
NFStream processes packets in timestamp order
      ↓
NFStream maintains an in-memory flow table
      ↓
Flow expires (FIN/RST or timeout)
      ↓
44 features computed
      ↓
JSON message
      ↓
Kafka topic "network-flows"
      ↓
(existing Spark + models + MongoDB + dashboard unchanged)
```

The producer must generate **exactly one Kafka message per completed flow**, never one message per packet.

Kafka is the contract boundary.

The JSON produced by the new producer must remain compatible with the existing Spark consumer so that **Spark, MongoDB and Streamlit require zero modifications**.

---

# GOAL

Create a new producer:

```
producer/pcap_kafka_producer.py
```

that replaces the CSV input with raw packet processing.

---

# IMPLEMENTATION REQUIREMENTS

## 1. Read the pcap correctly

Read a `.pcap` file using **NFStream** in offline mode:

```python
NFStreamer(source=<pcap_path>)
```

Packets must be processed in their **original timestamp order**.

The producer must conceptually behave as if packets are arriving live.

NFStream is responsible for:

- packet parsing

- bidirectional flow matching

- maintaining the in-memory flow table

- expiring flows

Do **not** treat the pcap as a static table of completed flows.

The intended lifecycle is:

```
packet arrives
      ↓
flow table updated
      ↓
another packet
      ↓
flow updated
      ↓
FIN/RST or timeout
      ↓
flow closes
      ↓
compute features
      ↓
Kafka message
```

---

## 2. Flow expiration

Let NFStream handle flow closing.

Unless the repository clearly specifies different values, use:

```
idle_timeout = 120 seconds
active_timeout = 120 seconds
```

These match CICFlowMeter defaults.

Before implementing, search the repository for configuration files, notes or documentation that specify different timeout values.

If none are found:

- use 120/120

- clearly document this assumption in the code

- clearly document this assumption in the final report

---

## 3. Feature extraction

The Random Forest model expects exactly the features listed in:

```
models/rf_features.json
```

Read this file **at runtime**.

Do **not** hardcode feature names.

Column names and ordering must match exactly.

For every completed flow:

- compute every feature from the pcap/NFStream output

- build the feature vector in the exact order specified in `rf_features.json`

The producer must **never** read `X_test.csv` to obtain feature values.

The CSV may only be used during the validation step described later.

---

## 4. Feature mapping

Create a mapping between:

```
NFStream attribute(s)
        ↓
required feature
```

For every feature produce documentation stating:

- feature name

- NFStream attribute(s) used

- exact mapping OR approximation

- formula used (if computed)

If NFStream cannot provide a feature directly:

- do NOT silently invent values

- do NOT silently fill with zero

- explain why it is unavailable

- explain whether NFPlugin could compute it

- if approximated, explain the approximation

Transparency is more important than perfect CICFlowMeter parity.

---

## 5. JSON format

Inspect the existing:

```
producer/kafka_producer.py
```

Reuse its JSON schema exactly.

The new producer must emit the same fields, including any metadata such as:

- timestamp

- flow_id

- feature dictionary

- model input fields

- any other metadata currently included

The downstream Spark consumer should not be able to distinguish whether the producer used CSV or pcap.

---

## 6. Kafka

Reuse the existing Kafka configuration from:

```
producer/kafka_producer.py
```

Reuse:

- bootstrap servers

- serializers

- topic name

Publish to:

```
network-flows
```

Do not reinvent the Kafka configuration.

---

## 7. Real-time replay

Replay the capture according to actual flow close times.

Let

```
t_i
```

be the close timestamp of flow *i*.

Before sending flow *i*:

```
sleep((t_i - t_(i-1)) / speed)
```

where

```
speed = 1.0
```

means real time.

Examples:

```
speed = 2
```

→ twice as fast

```
speed = 10
```

→ ten times faster

Never use a fixed sleep interval.

Replay timing must come from flow close timestamps.

---

## 8. Command-line interface

Provide:

```
--pcap
```

(required)

```
--limit
```

(maximum flows)

```
--speed
```

(default = 1.0)

Use the same CLI style as the existing producer.

---

## 9. Logging

For every flow sent print a concise log similar to the existing producer.

Include:

- timestamp

- 5-tuple

- packets

- bytes

- send success/failure

Keep logging lightweight.

---

# CONSTRAINTS

Do not modify:

- spark_streaming_prediction.py

- dashboard.py

- MongoDB schema

- Kafka topic

- trained models

- feature files

Create only:

```
producer/pcap_kafka_producer.py
```

Do not modify:

```
producer/kafka_producer.py
```

Use:

```
pip install nfstream
```

Do **not** write your own:

- packet parser

- TCP state tracker

- flow timeout engine

NFStream must perform flow management.

If NFPlugin is the cleanest solution for certain features, use it.

Perfect byte-for-byte equivalence with CICFlowMeter is **not** required.

Reasonable approximations are acceptable **only if every approximation is explicitly documented**.

---

# VALIDATION REQUIREMENTS

After implementation:

Run the producer against one pcap file that I will provide.

Kafka does **not** need to be running for this validation.

Instead:

Print the first 10 JSON messages that would be sent.

Then compare them against the closest matching rows in:

```
data/X_test.csv
```

using:

- timestamp

- or 5-tuple if available

For each comparison report:

- duration

- packets

- bytes

- first 10 feature values

- suspicious values

- missing features

- obvious inconsistencies

- whether values appear to be in the same numerical range

Also compute simple summary statistics such as:

- mean absolute difference (where comparison is possible)

- features with largest discrepancies

- features unavailable from NFStream

The goal is **sanity checking**, not exact equality.

---

# DELIVERABLES

## 1.

```
producer/pcap_kafka_producer.py
```

---

## 2.

```
producer/PCAP_PRODUCER_NOTES.md
```

containing:

- timeout settings used

- feature mapping table

- exact features

- approximated features

- unavailable features

- formulas used

- assumptions made

- suggested improvements

---

## 3.

Validation output showing:

- first 10 generated JSON messages

- comparison against X_test.csv

- summary of feature differences

- confirmation that no obviously invalid values were produced

---

# QUESTIONS TO ASK ME BEFORE STARTING

Do **not** begin implementation immediately.

First ask me the following questions and wait for my answers.

1. Where are the CICIDS2017 `.pcap` files located?

2. Is `models/rf_features.json` the only feature definition that must be matched, or should `xgb_features.json` also be considered?

3. Should Kafka receive flows for **all** traffic, or should any flows be filtered (for example incomplete flows remaining at the end of the capture)?

4. Does the repository contain any documentation describing preprocessing performed before model training that is not captured in `rf_features.json`?

Only after I answer these questions should implementation begin.

After implementation, complete the validation step before reporting that the task is finished.
