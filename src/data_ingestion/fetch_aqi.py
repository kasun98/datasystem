import requests
import json
import time
import os
from kafka_producer import KafkaDataProducer
from configs import KAFKA_TOPIC, STATION, API_KEY


# Kafka Configuration
producer = KafkaDataProducer(topic=KAFKA_TOPIC)


API_URL = f"https://api.waqi.info/feed/{STATION}/?token={API_KEY}"
TIME_INTERVAL = 300  # data fetch interval in seconds

def fetch_aqi():
    """Fetch AQI data from the API."""
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        # print(data)
        if "data" in data:
            aqi_info = {
                'aqi': data['data']['aqi'],
                'idx' : data['data']['idx'],
                'dominentpol': data['data']['dominentpol'],
                'iaqi_h': data['data']['iaqi']['h']['v'],
                'iaqi_t': data['data']['iaqi']['t']['v'],
                'iaqi_pm1': data['data']['iaqi']['pm1']['v'],
                'iaqi_pm10': data['data']['iaqi']['pm10']['v'],
                'iaqi_pm25': data['data']['iaqi']['pm25']['v'],
                'station': data['data']['city']['name'],
                'station_coords_lat': data['data']['city']['geo'][0],
                'station_coords_lon': data['data']['city']['geo'][1],
                'time': data['data']['time']['s']
            }
            return aqi_info
    return None

def stream_aqi():
    """Continuously fetch and send AQI data to Kafka."""
    while True:
        aqi_data = fetch_aqi()
        if aqi_data:
            producer.send_data(aqi_data)
            print(f"Sent to Kafka: {aqi_data['time']}")
        else:
            print("Failed to fetch AQI data.")
        time.sleep(TIME_INTERVAL)  

if __name__ == "__main__":
    stream_aqi()

