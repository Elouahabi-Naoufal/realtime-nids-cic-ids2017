"""
Live network traffic producer for IDS-Realtime.
Captures packets from a network interface, extracts CIC-IDS2017 features
using cicflowmeter, and sends each completed flow to Kafka.

Usage (requires root/sudo for packet capture):
    sudo venv/bin/python producer/kafka_producer_live.py --interface wlo1
"""

import argparse
import json
import threading
from kafka import KafkaProducer
from cicflowmeter.sniffer import create_sniffer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "network-flows"

# Mapping: cicflowmeter key → your model's feature name (44 features)
FEATURE_MAP = {
    "flow_duration":    "Flow Duration",
    "tot_fwd_pkts":     "Total Fwd Packets",
    "totlen_fwd_pkts":  "Total Length of Fwd Packets",
    "fwd_pkt_len_max":  "Fwd Packet Length Max",
    "fwd_pkt_len_min":  "Fwd Packet Length Min",
    "fwd_pkt_len_mean": "Fwd Packet Length Mean",
    "bwd_pkt_len_max":  "Bwd Packet Length Max",
    "bwd_pkt_len_min":  "Bwd Packet Length Min",
    "flow_byts_s":      "Flow Bytes/s",
    "flow_pkts_s":      "Flow Packets/s",
    "flow_iat_mean":    "Flow IAT Mean",
    "flow_iat_std":     "Flow IAT Std",
    "flow_iat_max":     "Flow IAT Max",
    "flow_iat_min":     "Flow IAT Min",
    "fwd_iat_mean":     "Fwd IAT Mean",
    "fwd_iat_std":      "Fwd IAT Std",
    "fwd_iat_min":      "Fwd IAT Min",
    "bwd_iat_tot":      "Bwd IAT Total",
    "bwd_iat_mean":     "Bwd IAT Mean",
    "bwd_iat_std":      "Bwd IAT Std",
    "bwd_iat_max":      "Bwd IAT Max",
    "bwd_iat_min":      "Bwd IAT Min",
    "fwd_psh_flags":    "Fwd PSH Flags",
    "fwd_header_len":   "Fwd Header Length",
    "bwd_header_len":   "Bwd Header Length",
    "bwd_pkts_s":       "Bwd Packets/s",
    "pkt_len_min":      "Min Packet Length",
    "pkt_len_max":      "Max Packet Length",
    "pkt_len_mean":     "Packet Length Mean",
    "pkt_len_var":      "Packet Length Variance",
    "fin_flag_cnt":     "FIN Flag Count",
    "psh_flag_cnt":     "PSH Flag Count",
    "ack_flag_cnt":     "ACK Flag Count",
    "urg_flag_cnt":     "URG Flag Count",
    "down_up_ratio":    "Down/Up Ratio",
    "init_fwd_win_byts":"Init_Win_bytes_forward",
    "init_bwd_win_byts":"Init_Win_bytes_backward",
    "fwd_act_data_pkts":"act_data_pkt_fwd",
    "fwd_seg_size_min": "min_seg_size_forward",
    "active_mean":      "Active Mean",
    "active_std":       "Active Std",
    "active_max":       "Active Max",
    "active_min":       "Active Min",
    "idle_std":         "Idle Std",
}

flow_counter = 0
counter_lock = threading.Lock()


class KafkaWriter:
    """Drop-in replacement for cicflowmeter's CSVWriter/HttpWriter.
    Receives completed flow dicts and sends them to Kafka."""

    def __init__(self, producer):
        self.producer = producer

    def write(self, data: dict) -> None:
        global flow_counter
        msg = {}
        for cic_key, model_key in FEATURE_MAP.items():
            msg[model_key] = float(data.get(cic_key, 0) or 0)

        with counter_lock:
            msg["flow_id"] = flow_counter
            flow_counter += 1

        msg["true_label"] = -1  # unknown — live traffic has no ground truth

        self.producer.send(TOPIC, msg)
        print(f"Flow {msg['flow_id']:>5} sent | "
              f"src={data.get('src_ip','?')}:{data.get('src_port','?')} → "
              f"dst={data.get('dst_ip','?')}:{data.get('dst_port','?')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interface", "-i", required=True,
                        help="Network interface to capture on (e.g. wlo1, eno2)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Patch cicflowmeter's FlowSession to use our KafkaWriter
    from cicflowmeter import flow_session as fs
    from cicflowmeter.writer import output_writer_factory as _orig

    kafka_writer = KafkaWriter(producer)

    def patched_factory(output_mode, output):
        return kafka_writer

    fs.output_writer_factory = patched_factory

    print(f"Starting live capture on interface: {args.interface}")
    print(f"Sending flows to Kafka topic: {TOPIC}")
    print("Press Ctrl+C to stop.\n")

    sniffer, session = create_sniffer(
        input_file=None,
        input_interface=args.interface,
        output_mode="csv",   # mode doesn't matter, writer is patched
        output="/dev/null",
    )
    sniffer.start()

    try:
        sniffer.join()
    except KeyboardInterrupt:
        sniffer.stop()
    finally:
        session.flush_flows()
        producer.flush()
        print(f"\nDone. {flow_counter} flows sent.")


if __name__ == "__main__":
    main()
