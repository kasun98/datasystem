# Real-Time Air Quality Prediction System

An end-to-end real-time data and ML pipeline for predicting Air Quality Index (AQI) using the WAQI API. This project uses Apache Kafka, Spark Structured Streaming, PostgreSQL, Airflow, MLflow, and Flask, all containerized using Docker Compose.

---

## Project Architecture

![Architecture Diagram](https://github.com/kasun98/datasystem/blob/main/architecture_diagram.jpg)

---

## Tech Stack

- **Data Ingestion**: WAQI API (every 5 min), Kafka Producer
- **Streaming & Processing**: Apache Kafka + Spark Structured Streaming
- **Data Storage**: PostgreSQL (Raw Data, Feature Store, Predictions)
- **Orchestration**: Apache Airflow
- **ML Experiment Tracking**: MLflow
- **Model Serving**: Flask API (Predict, Retrain, Reload, Test)
- **Containerization**: Docker Compose
- **Broker System**: Confluent Kafka (Broker, Zookeeper, Schema Registry, Control Center)

---

## ML Model (XGB)

- Experiments tracked using MLflow
- Retrained daily via Airflow scheduler
- Served using Flask app with following endpoints:
  - `/predict`
  - `/retrain`
  - `/reload`
  - `/test`
  - `/test_post`

---

## PostgreSQL Tables
0. **air_quality_db** - DB Schema
1. **air_quality_db.raw_data** – Unprocessed AQI data table
2. **air_quality_db.feature_store** – Engineered features for ML model table
3. **air_quality_db.predictions** – Final model predictions table

---

## Getting Started

### Prerequisites

- Python 3.10+
- Java (required for Spark)
- Apache Spark 3.5.5
- Docker & Docker Compose
- Virtual Environment

### Setup Instructions

```bash
# Clone the repo
git clone https://github.com/kasun98/datasystem.git
cd datasystem

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

- Get a **free API key** from [WAQI](https://aqicn.org/api/)
- Choose your station/location and get the **station ID**
- Rename the `rename.env` file to `.env`
- Update the `.env` file with:
  - API keys
  - PostgreSQL credentials
  - AQI station ID
  - Local paths, ...

---

## Spin Up Containers

```bash
docker-compose up -d
```

Containers:
- Kafka Broker
- Zookeeper
- Schema Registry
- Kafka Control Center
- Airflow Webserver, Scheduler, Postgres (for Airflow)
- Prometheus
- Optional: Spark Master/Worker, PostgreSQL DB, MLflow

---

## Running the Pipeline

### 0. Initialize postgres (onetime)
```bash
# Create postgreSQL tables
python src/data_ingestion/raw_database.py
```

### 1. Start Model API Server
```bash
python src/ml_model/app.py
```

### 2. Start Kafka Data Ingestion
```bash
python src/data_ingestion/fetch_aqi.py
```

### 3. Submit Spark Streaming Job
```bash
# Replace the <your_project_dir_abs_path> with Absolute path of your project directory 
spark-submit \
  --master local[*] \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.5 \
  --conf spark.pyspark.python="<your_project_dir_abs_path>/venv/Scripts/python.exe" \
  src/data_ingestion/spark_stream.py
```

---

## Monitoring & Visualization

```bash
# Replace the <your_project_dir_abs_path> with Absolute path of your project directory
mlflow ui --backend-store-uri "sqlite:///<replace_with_your_project_dir>/mlflow/mlflow.db"
```

| Tool        | URL                        | Description              |
|-------------|----------------------------|--------------------------|
| MLflow UI   | `http://localhost:5000`    | Experiment Tracking      |
| Airflow     | `http://localhost:8080`    | DAG Monitoring           |
| Spark UI    | `http://localhost:4040`    | Spark Jobs               |
| Confluent   | `http://localhost:9021`    | Kafka Monitoring         |
| Prometheus  | `http://localhost:9090`    | System Metrics           |

---

## Future Enhancements

- Integrate Grafana dashboards for visualization
- Add alerting and anomaly detection
- Deploy on cloud (AWS/GCP) for scalability
- Add analytics dashboard (Tableu/PowerBI)

---

## License

- MIT License – see LICENSE for details.

---

## Acknowledgments

-- WAQI API
-- Apache Spark, Airflow & Kafka
-- Confluent Platform
-- MLflow, Flask

## Author

**Kasun Dewaka**  
Feel free to reach out via [LinkedIn](https://linkedin.com/in/kasundewaka/) or open an issue!

