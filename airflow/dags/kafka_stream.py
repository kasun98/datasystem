# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator


# default_args = {
#     'owner': 'kdkas',
#     'start_date': datetime(2025, 4, 1, 17, 00)
# }


# def fetch_aqi():
#     import requests
#     """Fetch AQI data from the API."""
#     API_URL = f"https://api.waqi.info/feed/A132322/?token=00627c48e4351cd09ccb081b9879ab40cce3c7fa"
#     response = requests.get(API_URL)
#     if response.status_code == 200:
#         data = response.json()
#         # print(data)
#         if "data" in data:
#             aqi_info = {
#                 'aqi': data['data']['aqi'],
#                 'idx' : data['data']['idx'],
#                 'dominentpol': data['data']['dominentpol'],
#                 'iaqi_h': data['data']['iaqi']['h']['v'],
#                 'iaqi_t': data['data']['iaqi']['t']['v'],
#                 'iaqi_pm1': data['data']['iaqi']['pm1']['v'],
#                 'iaqi_pm10': data['data']['iaqi']['pm10']['v'],
#                 'iaqi_pm25': data['data']['iaqi']['pm25']['v'],
#                 'station': data['data']['city']['name'],
#                 'station_coords_lat': data['data']['city']['geo'][0],
#                 'station_coords_lon': data['data']['city']['geo'][1],
#                 'time': data['data']['time']['s']
#             }
#             return aqi_info
#     return None

# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import logging
#     producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    
#     try:
#         res = fetch_aqi()
#         producer.send("air_quality_info", json.dumps(res).encode('utf-8'))

#     except Exception as e:
#         logging.error(f'An error occured: {e}')
        

# with DAG('aqi_data_streaming',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )