# README.md

pip install -r requirements.txt


docker-compose up -d


python src/ml_model/app.py


python src/data_ingestion/fetch_aqi.py


spark-submit --master local[*] --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.5 --conf spark.pyspark.python="E:/projects/datasys/venv/Scripts/python.exe" src/data_ingestion/spark_stream.py


mlflow ui --backend-store-uri "sqlite:///E:/projects/datasys/mlflow/mlflow.db"