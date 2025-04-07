import os
from dotenv import load_dotenv

load_dotenv()

# AQI API Details  
API_KEY = os.getenv("AQI_API_KEY")
STATION = os.getenv("AQI_STATION")

# Kafka & PostgreSQL Config
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
POSTGRES_URL = "jdbc:postgresql://localhost:5432/air_quality_db"
POSTGRES_USER = os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD")
TABLE_NAME = "air_quality_data"
POSTGRES_JAR = os.getenv("POSTGRES_JAR")

# Database Config
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

# spark_stream configs
RAW_DATA_TABLE_NAME = os.getenv("RAW_DATA_TABLE_NAME")
FEATURE_STORE_TABLE_NAME = os.getenv("FEATURE_STORE_TABLE_NAME")
PREDICTION_TABLE_NAME = os.getenv("PREDICTION_TABLE_NAME")
MODEL_PREDICTION_ENDPOINT = os.getenv("MODEL_PREDICTION_ENDPOINT")







