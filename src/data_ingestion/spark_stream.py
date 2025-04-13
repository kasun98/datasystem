import logging
import requests
import json
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour, dayofweek, month, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType
from configs import KAFKA_TOPIC, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, RAW_DATA_TABLE_NAME, FEATURE_STORE_TABLE_NAME, PREDICTION_TABLE_NAME, MODEL_PREDICTION_ENDPOINT

FEATURES = ["pm1", "pm10", "pm25", "rh", "temp", "hour", "day_of_week", "month", "is_weekend", "temp_x_rh", "pm1_x_pm10", "pm1_x_pm25", "pm10_x_pm25"]

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.postgresql:postgresql:42.7.5," \
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return s_conn

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', KAFKA_TOPIC) \
            .option('startingOffsets', 'latest') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("aqi", IntegerType(), True),
        StructField("idx", IntegerType(), True),
        StructField("dominentpol", StringType(), True),
        StructField("iaqi_h", FloatType(), True),
        StructField("iaqi_t", FloatType(), True),
        StructField("iaqi_pm1", FloatType(), True),
        StructField("iaqi_pm10", FloatType(), True),
        StructField("iaqi_pm25", FloatType(), True),
        StructField("station", StringType(), True),
        StructField("station_coords_lat", DoubleType(), True),
        StructField("station_coords_lon", DoubleType(), True),
        StructField("time", TimestampType(), True)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
    
    return sel  

def transform_to_feature_store(df):
    return df.withColumn("location_id", col("station")) \
        .withColumn("date", col("time")) \
        .withColumn("pm1", col("iaqi_pm1")) \
        .withColumn("pm10", col("iaqi_pm10")) \
        .withColumn("pm25", col("iaqi_pm25")) \
        .withColumn("rh", col("iaqi_h")) \
        .withColumn("temp", col("iaqi_t")) \
        .withColumn("hour", hour(col("time"))) \
        .withColumn("day_of_week", dayofweek(col("time"))) \
        .withColumn("month", month(col("time"))) \
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), True).otherwise(False)) \
        .withColumn("temp_x_rh", col("temp") * col("rh")) \
        .withColumn("pm1_x_pm10", col("pm1") * col("pm10")) \
        .withColumn("pm1_x_pm25", col("pm1") * col("pm25")) \
        .withColumn("pm10_x_pm25", col("pm10") * col("pm25")) \
        .withColumn("created_at", current_timestamp()) \
        .select("location_id", "date", "pm1", "pm10", "pm25", "rh", "temp", "hour", "day_of_week", "month", "is_weekend", "temp_x_rh", "pm1_x_pm10", "pm1_x_pm25", "pm10_x_pm25", "created_at")

def send_data_to_predict_api(df_pandas):
    payload = {
        "columns": df_pandas.columns.tolist(),
        "data": df_pandas.values.tolist()
    }
    logging.info(f"Payload for prediction: {payload}")
    try:
        response = requests.post(MODEL_PREDICTION_ENDPOINT, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to get predictions. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending request to prediction API: {e}")
        return None
    

def write_to_postgres_jdbc(batch_df, epoch_id, table_name):
    if batch_df.isEmpty():
        logging.info(f"Empty batch, skipping write operation for {table_name}.")
        return
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_HOST}/{DB_NAME}") \
            .option("dbtable", table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logging.info(f"Batch {epoch_id} written to {table_name} successfully!")
    except Exception as e:
        logging.error(f"Failed to write batch {epoch_id} to {table_name}: {e}")


def process_batch(batch_df, epoch_id):
    if batch_df.isEmpty():
        return
    
    # Write raw data
    try:
        write_to_postgres_jdbc(batch_df, epoch_id, RAW_DATA_TABLE_NAME)
    except Exception as e:
        logging.error(f"Failed to write raw data: {e}")

    # Transform for feature store
    try:
        feature_df = transform_to_feature_store(batch_df)
        write_to_postgres_jdbc(feature_df, epoch_id, FEATURE_STORE_TABLE_NAME)
        pandas_df_ = feature_df.toPandas()
        pandas_df = pandas_df_[FEATURES].copy()
        pandas_df['is_weekend'] = pandas_df['is_weekend'].astype(int)

    except Exception as e:
        logging.error(f"Failed to transform and write feature store data: {e}")

    # send to prediction API
    try:
        predictions = send_data_to_predict_api(pandas_df)
        predicted_time = datetime.now()
    except Exception as e:
        logging.error(f"Failed to send data to prediction API: {e}")
        predictions = None
    
    if predictions is not None:
        # Parse predictions from the API response
        prediction_values = predictions.get('prediction', [])[0]
        logging.info(f"Predictions: {prediction_values}")
        
        # Add the predictions to the dataframe
        final_predicted_df = pd.DataFrame({
            "location_id": pandas_df_["location_id"],
            "date": pandas_df_["date"],
            "predicted_at": predicted_time,
            "pm25_t_5min": prediction_values[0],
            "pm25_t_10min": prediction_values[1],
            "pm25_t_15min": prediction_values[2],
            "pm25_t_20min": prediction_values[3],
            "pm25_t_25min": prediction_values[4],
            "pm25_t_30min": prediction_values[5]
        })

        # Convert back to Spark DF and write predictions
        spark_predicted_df = spark.createDataFrame(final_predicted_df)
        try:
            write_to_postgres_jdbc(spark_predicted_df, epoch_id, PREDICTION_TABLE_NAME)
        except Exception as e:
            logging.error(f"Failed to write predictions: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = create_spark_connection()

    if spark is not None:
        kafka_df = connect_to_kafka(spark)
        if kafka_df is not None:
            selection_df = create_selection_df_from_kafka(kafka_df)
            query = selection_df.writeStream \
                .foreachBatch(process_batch) \
                .start()

            query.awaitTermination()