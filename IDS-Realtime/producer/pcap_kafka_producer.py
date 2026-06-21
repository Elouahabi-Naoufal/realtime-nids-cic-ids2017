"""
producer/pcap_kafka_producer.py

Reads a raw .pcap file using NFStream, extracts exactly the 44 features
expected by the RF/XGBoost/DL models, and publishes one JSON message per
completed flow to the Kafka topic "network-flows".

The JSON schema is identical to kafka_producer.py so Spark requires zero changes.

Timeout assumptions (no repo-level config found — using CICFlowMeter defaults):
    idle_timeout  = 120 s
    active_timeout = 120 s

Usage:
    venv/bin/python producer/pcap_kafka_producer.py \
        --pcap /path/to/file.pcap [--limit N] [--speed 1.0] [--dry-run]
"""

import argparse
import json
import time
import math
from kafka import KafkaProducer
from nfstream import NFStreamer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC           = "network-flows"

# Idle/active timeout in seconds — CICFlowMeter defaults
IDLE_TIMEOUT_S   = 120
ACTIVE_TIMEOUT_S = 120

# ---------------------------------------------------------------------------
# NFStream attribute → CIC-IDS2017 feature mapping
#
# src2dst = forward (client→server)
# dst2src = backward (server→client)
# All piat values are in ms → converted to µs to match CICFlowMeter convention
# All ps (packet size) values are in bytes
# ---------------------------------------------------------------------------
def extract_features(f, feature_names: list) -> dict:
    """Map NFStream flow f to the 44 CIC-IDS2017 features."""
    ms = 1_000   # ms → µs multiplier for IAT fields

    # Derived / approximated values
    duration_us     = f.bidirectional_duration_ms * 1_000
    total_fwd_pkts  = f.src2dst_packets
    total_bwd_pkts  = f.dst2src_packets
    total_pkts      = f.bidirectional_packets
    duration_s      = max(f.bidirectional_duration_ms / 1_000, 1e-9)

    flow_bytes_s    = f.bidirectional_bytes   / duration_s
    flow_pkts_s     = total_pkts              / duration_s
    bwd_pkts_s      = total_bwd_pkts          / duration_s

    # Packet length variance = stddev²
    pkt_len_var = (f.bidirectional_stddev_ps ** 2) if f.bidirectional_stddev_ps else 0

    # Header lengths: NFStream does not expose header bytes directly.
    # Approximation: TCP header = 20 bytes, IPv4 header = 20 bytes → 40 bytes per packet.
    # APPROXIMATION — documented in PCAP_PRODUCER_NOTES.md
    fwd_header_len = total_fwd_pkts * 40
    bwd_header_len = total_bwd_pkts * 40

    # min_seg_size_forward: NFStream does not expose TCP segment size.
    # Approximation: use minimum forward packet size.
    # APPROXIMATION — documented in PCAP_PRODUCER_NOTES.md
    min_seg_size_fwd = f.src2dst_min_ps if f.src2dst_min_ps else 0

    # act_data_pkt_fwd: packets with payload in forward direction.
    # NFStream does not expose this directly.
    # Approximation: total forward packets (upper bound).
    # APPROXIMATION — documented in PCAP_PRODUCER_NOTES.md
    act_data_pkt_fwd = total_fwd_pkts

    # Bwd IAT Total: NFStream gives mean/std/min/max but not total.
    # Approximation: mean_piat * (bwd_packets - 1)
    bwd_n_iats = max(total_bwd_pkts - 1, 0)
    bwd_iat_total = f.dst2src_mean_piat_ms * ms * bwd_n_iats

    # Active/Idle: NFStream does not expose active/idle time series.
    # All active/idle features set to 0.
    # NOT APPROXIMATED — unavailable; documented in PCAP_PRODUCER_NOTES.md

    raw = {
        "Flow Duration":               duration_us,
        "Total Fwd Packets":           total_fwd_pkts,
        "Total Length of Fwd Packets": f.src2dst_bytes,
        "Fwd Packet Length Max":       f.src2dst_max_ps,
        "Fwd Packet Length Min":       f.src2dst_min_ps,
        "Fwd Packet Length Mean":      f.src2dst_mean_ps,
        "Bwd Packet Length Max":       f.dst2src_max_ps,
        "Bwd Packet Length Min":       f.dst2src_min_ps,
        "Flow Bytes/s":                flow_bytes_s,
        "Flow Packets/s":              flow_pkts_s,
        "Flow IAT Mean":               f.bidirectional_mean_piat_ms  * ms,
        "Flow IAT Std":                f.bidirectional_stddev_piat_ms * ms,
        "Flow IAT Max":                f.bidirectional_max_piat_ms   * ms,
        "Flow IAT Min":                f.bidirectional_min_piat_ms   * ms,
        "Fwd IAT Mean":                f.src2dst_mean_piat_ms        * ms,
        "Fwd IAT Std":                 f.src2dst_stddev_piat_ms      * ms,
        "Fwd IAT Min":                 f.src2dst_min_piat_ms         * ms,
        "Bwd IAT Total":               bwd_iat_total,
        "Bwd IAT Mean":                f.dst2src_mean_piat_ms        * ms,
        "Bwd IAT Std":                 f.dst2src_stddev_piat_ms      * ms,
        "Bwd IAT Max":                 f.dst2src_max_piat_ms         * ms,
        "Bwd IAT Min":                 f.dst2src_min_piat_ms         * ms,
        "Fwd PSH Flags":               f.src2dst_psh_packets,
        "Fwd Header Length":           fwd_header_len,
        "Bwd Header Length":           bwd_header_len,
        "Bwd Packets/s":               bwd_pkts_s,
        "Min Packet Length":           f.bidirectional_min_ps,
        "Max Packet Length":           f.bidirectional_max_ps,
        "Packet Length Mean":          f.bidirectional_mean_ps,
        "Packet Length Variance":      pkt_len_var,
        "FIN Flag Count":              f.bidirectional_fin_packets,
        "PSH Flag Count":              f.bidirectional_psh_packets,
        "ACK Flag Count":              f.bidirectional_ack_packets,
        "URG Flag Count":              f.bidirectional_urg_packets,
        "Down/Up Ratio":               total_bwd_pkts / max(total_fwd_pkts, 1),
        "Init_Win_bytes_forward":      0,   # not exposed by NFStream
        "Init_Win_bytes_backward":     0,   # not exposed by NFStream
        "act_data_pkt_fwd":            act_data_pkt_fwd,
        "min_seg_size_forward":        min_seg_size_fwd,
        "Active Mean":                 0,   # unavailable — NFStream has no active/idle series
        "Active Std":                  0,
        "Active Max":                  0,
        "Active Min":                  0,
        "Idle Std":                    0,
    }

    # Sanitize: replace inf/nan with 0
    for k, v in raw.items():
        if not isinstance(v, (int, float)) or math.isnan(v) or math.isinf(v):
            raw[k] = 0

    # Return in exact feature_names order
    return {feat: raw[feat] for feat in feature_names}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcap",  required=True, help="Path to .pcap file")
    parser.add_argument("--limit", type=int,   default=None,  help="Max flows to send")
    parser.add_argument("--speed", type=float, default=1.0,   help="Replay speed multiplier")
    parser.add_argument("--dry-run", action="store_true",     help="Print JSON without sending to Kafka")
    args = parser.parse_args()

    # Load feature list at runtime — never hardcoded
    with open("models/rf_features.json") as f:
        feature_names = json.load(f)

    producer = None
    if not args.dry_run:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    streamer = NFStreamer(
        source=args.pcap,
        idle_timeout=IDLE_TIMEOUT_S,
        active_timeout=ACTIVE_TIMEOUT_S,
        statistical_analysis=True,
    )

    flow_id      = 0
    last_close_ms = None

    for flow in streamer:
        if args.limit and flow_id >= args.limit:
            break

        close_ms = flow.bidirectional_last_seen_ms

        # Real-time replay based on flow close timestamps
        if last_close_ms is not None and args.speed > 0:
            delta_s = (close_ms - last_close_ms) / 1_000 / args.speed
            if delta_s > 0:
                time.sleep(delta_s)
        last_close_ms = close_ms

        features = extract_features(flow, feature_names)

        msg = {
            "flow_id":    flow_id,
            "true_label": -1,           # pcap has no ground truth labels
            **features,
        }

        if args.dry_run:
            print(f"[Flow {flow_id:>4}] {flow.src_ip}:{flow.src_port} → "
                  f"{flow.dst_ip}:{flow.dst_port} | "
                  f"pkts={flow.bidirectional_packets} bytes={flow.bidirectional_bytes}")
            if flow_id < 10:
                print(json.dumps(msg, indent=2))
        else:
            producer.send(TOPIC, msg)
            print(f"[{flow_id:>5}] {flow.src_ip}:{flow.src_port}→{flow.dst_ip}:{flow.dst_port} "
                  f"pkts={flow.bidirectional_packets} bytes={flow.bidirectional_bytes} ✓")

        flow_id += 1

    if producer:
        producer.flush()
        print(f"\nDone. {flow_id} flows sent to Kafka topic '{TOPIC}'.")
    else:
        print(f"\nDry run complete. {flow_id} flows processed.")


if __name__ == "__main__":
    main()
