<a id="readme-top"></a>

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![LinkedIn][linkedin-shield]][linkedin-url]

<br />
<div align="center">
  <a href="https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017">
    <h3 align="center">Real-Time Network Intrusion Detection on CIC-IDS2017</h3>
  </a>

  <p align="center">
    Kafka → Spark Streaming → Random Forest / XGBoost / CNN-BiLSTM → MongoDB → Streamlit.
    <br />
    <a href="https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017"><strong>Explore the repository »</strong></a>
    <br />
    <br />
    <a href="https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/issues/new?labels=bug">Report Bug</a>
    &middot;
    <a href="https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/issues/new?labels=enhancement">Request Feature</a>
  </p>
</div>

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#pipeline">Pipeline</a></li>
        <li><a href="#models-and-decision-logic">Models and decision logic</a></li>
        <li><a href="#repository-layout">Repository layout</a></li>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

## About The Project

This is a fourth-year PFA project at EMSI: a real-time network intrusion detection system built on
CIC-IDS2017. The repository combines the model-training notebooks with a runnable streaming demo
that scores network flows as they arrive.

The streaming path is deliberately split at Kafka. Producers publish one JSON message per flow to
the `network-flows` topic; everything downstream consumes that contract, so a CSV replay producer
and a raw-PCAP producer can drive the same pipeline.

### Pipeline

```text
CSV replay (data/X_test.csv) ─┐
                              ├─► Kafka topic "network-flows"
raw .pcap ──► NFStream ───────┘
                                      │
                                      v
                         Spark Structured Streaming
                    (foreachBatch, checkpointed on disk)
                                      │
                                      v
                   Random Forest → XGBoost / CNN-BiLSTM
                                      │
                                      v
                        MongoDB  ids_db.predictions
                                      │
                                      v
                      Streamlit dashboard  :8501
```

Infrastructure (Kafka, ZooKeeper, MongoDB) is defined in `IDS-Realtime/compose.yaml`. Spark and the
dashboard run as local processes, not as Compose services.

### Models and decision logic

Three trained artifacts are committed:

* **Random Forest** (`random_forest_multiclass_model.joblib`) with its label encoder and feature list.
* **XGBoost** (`xgboost_multiclass_final.ubj`) with a label encoder; the attack-only variant was
  trained on 44 flow features.
* **CNN-BiLSTM-Transformer** (`cnn_bilstm_transformer_multiclass_v5.keras`), a multiclass model
  over 11 attack families.

`spark/spark_streaming_prediction.py` uses them in stages: Random Forest classifies every flow
first; if it predicts `BENIGN`, the result is written as `NORMAL`. Otherwise the flow is marked
`ATTACK` and XGBoost plus the deep model assign the attack type.

`IDS-Realtime/results_xgb_same_split_44/metadata_xgb_same_split_44.json` records the XGBoost
run on the same split as the Random Forest: accuracy `0.9975`, macro-F1 `0.9295`, weighted-F1
`0.9975`. These are the metrics stored with that artifact, not a cross-project benchmark.

### Repository layout

```text
.
├── notebooks/
│   ├── exploration_cicids2017.ipynb
│   ├── RANDOMFORESTMULTICLASS_CIC.ipynb
│   ├── XGboost-attacks.ipynb
│   ├── cnn_bilstm_binary_v3.ipynb
│   └── cnn_bilstm_multiclass_v5_fixed.ipynb
├── *.ipynb                     # top-level copies of the same training notebooks
└── IDS-Realtime/
    ├── compose.yaml            # Kafka + ZooKeeper + MongoDB
    ├── requirements.txt
    ├── producer/
    │   ├── kafka_producer.py        # replay rows from data/X_test.csv
    │   ├── kafka_producer_live.py   # live capture path
    │   └── pcap_kafka_producer.py   # raw PCAP → flows with NFStream
    ├── spark/
    │   └── spark_streaming_prediction.py
    ├── models/                 # trained artifacts, encoders and feature lists
    ├── ui/
    │   └── dashboard.py        # Streamlit live monitor
    ├── scripts/                # Windows PowerShell start scripts
    └── data/                   # X_test.csv, y_test.csv, y_train.csv
```

The top-level notebooks duplicate the files under `notebooks/`; the streaming code is entirely in
`IDS-Realtime/`.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

* [![Apache Kafka][Kafka-badge]][Kafka-url]
* [![Apache Spark][Spark-badge]][Spark-url]
* [![MongoDB][MongoDB-badge]][MongoDB-url]
* [![Streamlit][Streamlit-badge]][Streamlit-url]
* [![scikit-learn][scikit-learn-badge]][scikit-learn-url]
* [![XGBoost][XGBoost-badge]][XGBoost-url]
* [![Keras][Keras-badge]][Keras-url]
* [![PyTorch][PyTorch-badge]][PyTorch-url]
* [![Docker][Docker-badge]][Docker-url]
* [![Python][Python-badge]][Python-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started

### Prerequisites

* Docker Engine with the Compose plugin, for Kafka, ZooKeeper and MongoDB.
* Python 3.10+.
* A Spark installation compatible with the pinned version (`pyspark==4.1.2` in
  `IDS-Realtime/requirements.txt`). The provided start script assumes a Windows setup with a local
  Hadoop/winutils directory; on Linux or macOS use the `spark-submit` command directly.
* Java 17+ for Spark.

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017.git
   cd realtime-nids-cic-ids2017/IDS-Realtime
   ```

2. Install the Python dependencies
   ```sh
   python -m venv .venv
   source .venv/bin/activate        # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Start the infrastructure
   ```sh
   docker compose up -d
   ```
   The `kafka-init` service waits for the broker and creates the `network-flows` topic if it does
   not exist. MongoDB listens on `27017`, Kafka on `localhost:9092`.

4. Confirm the topic exists
   ```sh
   docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

The demo has four processes: Spark, a producer, the dashboard, and the infrastructure from
Compose. Run Spark and the dashboard from `IDS-Realtime/`.

### 1. Start Spark Streaming

Linux / macOS:

```sh
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
  spark/spark_streaming_prediction.py
```

Windows:

```powershell
.\scripts\01_start_spark.ps1
```

The job reads from `network-flows`, batches with `foreachBatch`, and writes predictions to
`ids_db.predictions`. Checkpoints are stored in `checkpoints/ids_stream`.

### 2. Start the dashboard

```sh
streamlit run ui/dashboard.py
```

Open <http://localhost:8501>. The sidebar lets you change the MongoDB URI, database and collection,
set the refresh interval, and clear the predictions collection.

### 3. Publish flows

Replay a slice of the preprocessed test set:

```sh
python producer/kafka_producer.py --limit 500 --sleep 0.1
```

Replay a raw capture, one message per completed flow:

```sh
python producer/pcap_kafka_producer.py --pcap /path/to/capture.pcap --limit 200 --speed 1.0
```

Add `--dry-run` to the PCAP producer to inspect the extracted messages without publishing. The
feature mapping and the NFStream approximations are documented in
`producer/PCAP_PRODUCER_NOTES.md`.

### 4. One-command demo (Windows)

```powershell
.\start_base_demo.ps1
```

This starts Compose, clears the predictions collection, launches Spark and Streamlit in separate
windows, and leaves the producer for you to start manually.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Roadmap

- [x] Explore CIC-IDS2017 and train Random Forest, XGBoost and CNN-BiLSTM models
- [x] Package the trained artifacts with their encoders and feature lists
- [x] Kafka → Spark Streaming → MongoDB pipeline
- [x] Streamlit live dashboard
- [x] CSV replay producer and raw-PCAP producer
- [ ] Replace the Windows-specific paths in `scripts/` with portable configuration
- [ ] Containerise Spark and the dashboard so the demo no longer needs host-side setup
- [ ] Remove committed `__pycache__` and Spark checkpoint files from version control
- [ ] Add automated tests for the feature-preparation and label-decoding functions
- [ ] Document the train/test split and evaluation protocol for each committed model

See the [open issues](https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

Keep the Kafka message schema stable. Both producers and the Spark consumer depend on the same
JSON field names.

### Top contributors:

<a href="https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Elouahabi-Naoufal/realtime-nids-cic-ids2017" alt="contrib.rocks image" />
</a>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## License

This project is not licensed — all rights reserved.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contact

Naoufal Elouahabi - [LinkedIn](https://www.linkedin.com/in/elouahabi-naoufal/) - elouahabi-naoufal1@gmail.com

Project Link: [https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017](https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Acknowledgments

* [CIC-IDS2017 dataset](https://www.unb.ca/cic/datasets/ids-2017.html) — Canadian Institute for Cybersecurity, University of New Brunswick.
* [CICFlowMeter](https://github.com/ahlashkari/CICFlowMeter) — the flow-feature convention the models were trained on.
* [NFStream](https://www.nfstream.org/) — flow extraction for the PCAP producer.
* [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
* [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
* [Streamlit Documentation](https://docs.streamlit.io/)
* [Img Shields](https://shields.io)
* [Choose an Open Source License](https://choosealicense.com)
* [Best-README-Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

[contributors-shield]: https://img.shields.io/github/contributors/Elouahabi-Naoufal/realtime-nids-cic-ids2017.svg?style=for-the-badge
[contributors-url]: https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/Elouahabi-Naoufal/realtime-nids-cic-ids2017.svg?style=for-the-badge
[forks-url]: https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/network/members
[stars-shield]: https://img.shields.io/github/stars/Elouahabi-Naoufal/realtime-nids-cic-ids2017.svg?style=for-the-badge
[stars-url]: https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/stargazers
[issues-shield]: https://img.shields.io/github/issues/Elouahabi-Naoufal/realtime-nids-cic-ids2017.svg?style=for-the-badge
[issues-url]: https://github.com/Elouahabi-Naoufal/realtime-nids-cic-ids2017/issues
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/elouahabi-naoufal/
[Kafka-badge]: https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white
[Kafka-url]: https://kafka.apache.org/
[Spark-badge]: https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white
[Spark-url]: https://spark.apache.org/
[MongoDB-badge]: https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white
[MongoDB-url]: https://www.mongodb.com/
[Streamlit-badge]: https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white
[Streamlit-url]: https://streamlit.io/
[scikit-learn-badge]: https://img.shields.io/badge/scikit--learn-F7931E?style=for-the-badge&logo=scikitlearn&logoColor=white
[scikit-learn-url]: https://scikit-learn.org/
[XGBoost-badge]: https://img.shields.io/badge/XGBoost-189FDD?style=for-the-badge&logo=xgboost&logoColor=white
[XGBoost-url]: https://xgboost.readthedocs.io/
[Keras-badge]: https://img.shields.io/badge/Keras-D00000?style=for-the-badge&logo=keras&logoColor=white
[Keras-url]: https://keras.io/
[PyTorch-badge]: https://img.shields.io/badge/PyTorch-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white
[PyTorch-url]: https://pytorch.org/
[Docker-badge]: https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white
[Docker-url]: https://www.docker.com/
[Python-badge]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org/
