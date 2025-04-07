from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def trigger_model_retrain():
    try:
        response = requests.post("http://192.168.8.154:5050/retrain") # change to your API endpoint
        if response.status_code == 200:
            print("Retrain triggered successfully!")
        else:
            print("Failed to trigger retrain. Status code:", response.status_code)
        print("Status:", response.status_code)
        print("Response:", response.text)
    except requests.exceptions.RequestException as e:
        print("Error sending request to retrain API:", e)

with DAG(
    dag_id="daily_model_retrain",
    start_date=datetime(2025, 4, 4),
    schedule_interval="0 0 * * *",  # Every day at midnight
    catchup=False,
) as dag:

    retrain_task = PythonOperator(
        task_id="trigger_retrain_api",
        python_callable=trigger_model_retrain,
    )
