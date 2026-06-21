# PCAP Producer — Implementation Notes

## Timeout Settings

| Parameter | Value | Source |
|---|---|---|
| `idle_timeout` | 120 s | CICFlowMeter default (no repo config found) |
| `active_timeout` | 120 s | CICFlowMeter default (no repo config found) |

---

## Feature Mapping Table

### Exact mappings (no approximation)

| CIC-IDS2017 Feature | NFStream Attribute | Notes |
|---|---|---|
| Flow Duration | `bidirectional_duration_ms × 1000` | ms → µs |
| Total Fwd Packets | `src2dst_packets` | |
| Total Length of Fwd Packets | `src2dst_bytes` | |
| Fwd Packet Length Max | `src2dst_max_ps` | |
| Fwd Packet Length Min | `src2dst_min_ps` | |
| Fwd Packet Length Mean | `src2dst_mean_ps` | |
| Bwd Packet Length Max | `dst2src_max_ps` | |
| Bwd Packet Length Min | `dst2src_min_ps` | |
| Flow Bytes/s | `bidirectional_bytes / duration_s` | |
| Flow Packets/s | `bidirectional_packets / duration_s` | |
| Flow IAT Mean | `bidirectional_mean_piat_ms × 1000` | ms → µs |
| Flow IAT Std | `bidirectional_stddev_piat_ms × 1000` | ms → µs |
| Flow IAT Max | `bidirectional_max_piat_ms × 1000` | ms → µs |
| Flow IAT Min | `bidirectional_min_piat_ms × 1000` | ms → µs |
| Fwd IAT Mean | `src2dst_mean_piat_ms × 1000` | ms → µs |
| Fwd IAT Std | `src2dst_stddev_piat_ms × 1000` | ms → µs |
| Fwd IAT Min | `src2dst_min_piat_ms × 1000` | ms → µs |
| Bwd IAT Mean | `dst2src_mean_piat_ms × 1000` | ms → µs |
| Bwd IAT Std | `dst2src_stddev_piat_ms × 1000` | ms → µs |
| Bwd IAT Max | `dst2src_max_piat_ms × 1000` | ms → µs |
| Bwd IAT Min | `dst2src_min_piat_ms × 1000` | ms → µs |
| Fwd PSH Flags | `src2dst_psh_packets` | |
| Bwd Packets/s | `dst2src_packets / duration_s` | |
| Min Packet Length | `bidirectional_min_ps` | |
| Max Packet Length | `bidirectional_max_ps` | |
| Packet Length Mean | `bidirectional_mean_ps` | |
| Packet Length Variance | `bidirectional_stddev_ps²` | stddev² |
| FIN Flag Count | `bidirectional_fin_packets` | |
| PSH Flag Count | `bidirectional_psh_packets` | |
| ACK Flag Count | `bidirectional_ack_packets` | |
| URG Flag Count | `bidirectional_urg_packets` | |
| Down/Up Ratio | `dst2src_packets / src2dst_packets` | |

---

### Approximated features

| CIC-IDS2017 Feature | Approximation | Reason |
|---|---|---|
| Fwd Header Length | `src2dst_packets × 40` | NFStream does not expose raw header bytes. 40 bytes = 20B IPv4 + 20B TCP (minimum). Actual headers may be larger (options). |
| Bwd Header Length | `dst2src_packets × 40` | Same reason as above. |
| Bwd IAT Total | `dst2src_mean_piat_ms × (dst2src_packets - 1) × 1000` | NFStream exposes mean/std/min/max but not the sum of all IATs. |
| act_data_pkt_fwd | `src2dst_packets` | NFStream does not distinguish packets with payload from pure ACKs. Upper bound used. |
| min_seg_size_forward | `src2dst_min_ps` | NFStream does not expose TCP segment size separately from IP packet size. |

---

### Unavailable features (set to 0)

| CIC-IDS2017 Feature | Reason |
|---|---|
| Init_Win_bytes_forward | NFStream does not expose TCP window size from the SYN packet. Would require NFPlugin with raw TCP header inspection. |
| Init_Win_bytes_backward | Same as above. |
| Active Mean | NFStream does not compute active/idle time series. CICFlowMeter splits a flow into active and idle periods based on a 5s inactivity threshold. This would require a custom NFPlugin. |
| Active Std | Same as above. |
| Active Max | Same as above. |
| Active Min | Same as above. |
| Idle Std | Same as above. |

---

## Assumptions

1. IAT values from NFStream are in milliseconds; CIC-IDS2017 uses microseconds. Multiplied by 1000.
2. Flow duration from NFStream is in milliseconds; CIC-IDS2017 uses microseconds. Multiplied by 1000.
3. `src2dst` = forward direction (initiator → responder).
4. `dst2src` = backward direction (responder → initiator).
5. Timeout values of 120/120s are assumed. No configuration was found in the repository.

---

## Suggested Improvements

1. Implement a custom `NFPlugin` to track TCP initial window size from SYN packets.
2. Implement a custom `NFPlugin` to compute active/idle periods using a 5-second inactivity threshold (matching CICFlowMeter).
3. Use `act_data_pkt_fwd` accurately by tracking packets with `len(payload) > 0` in a plugin.
4. Cross-validate feature distributions against `data/X_test.csv` per attack class.
