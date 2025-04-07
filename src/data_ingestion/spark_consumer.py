# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, to_timestamp
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DoubleType, TimestampType
# from configs import KAFKA_TOPIC, KAFKA_BROKER, POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_JAR, TABLE_NAME


# # Define Schema for Kafka Data
# schema = StructType([
#         StructField("aqi", IntegerType(), True),
#         StructField("idx", IntegerType(), True),
#         StructField("dominentpol", StringType(), True),
#         StructField("iaqi_h", FloatType(), True),
#         StructField("iaqi_t", FloatType(), True),
#         StructField("iaqi_pm1", FloatType(), True),
#         StructField("iaqi_pm10", FloatType(), True),
#         StructField("iaqi_pm25", FloatType(), True),
#         StructField("station", StringType(), True),
#         StructField("station_coords_lat", DoubleType(), True),
#         StructField("station_coords_lon", DoubleType(), True),
#         StructField("time", TimestampType(), True)
#     ])


# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("KafkaSparkConsumer") \
#     .config("spark.jars", POSTGRES_JAR) \
#     .getOrCreate()

# # Read Kafka Stream
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "latest") \
#     .load()

# # Deserialize JSON messages
# df = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # Convert timestamp column to proper format
# df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# # Store Data in PostgreSQL
# def write_to_postgres(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", POSTGRES_URL) \
#         .option("dbtable", TABLE_NAME) \
#         .option("user", POSTGRES_USER) \
#         .option("password", POSTGRES_PASSWORD) \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# # Write stream to PostgreSQL
# query = df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()
