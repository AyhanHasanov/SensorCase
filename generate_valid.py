import os
import json
import random
import time
from datetime import datetime
from rich import print

sensors = ["sensor1", "sensor2", "sensor3", "sensor4"]
data_dir = "data/raw"
stable_dir = "data/stable"
error_dir = "data/error"
os.makedirs(stable_dir, exist_ok=True)
os.makedirs(error_dir, exist_ok=True)
generate_every = 5

def generate_sensor_data():
    while True:
        error_data = []
        stable_data = []
        timestamp = datetime.now()
        file_name = f"Sensor{timestamp.strftime("%Y%m%d-%H%M%S")}.json"

        for sensor in sensors :
            temperature = random.uniform(10.0, 15.0) if random.random() > 0.04 else -100
            frequency = random.uniform(15, 150) if random.random() > 0.04 else -100
            status = "Error" if temperature == -100 or frequency == -100 else "Stable"

            to_append = {
                "sensor_id" : sensor,
                "timestamp" : timestamp.isoformat(),
                "temperature" : round(temperature, 2),
                "frequency" : round(frequency, 2),
                "status" : status
                }

            if status == "Error":
                print(f"\x1b[93mWARNING: {sensor} returned an error at {timestamp.isoformat()}\x1b[0m")
                error_data.append(to_append)
                continue

            stable_data.append(to_append)

        stable_data_path = os.path.join(stable_dir, file_name)
        with open(stable_data_path, "w") as f :
            json.dump(stable_data, f)

        if len(error_data) != 0:
            error_data_path = os.path.join(error_dir, file_name)
            with open(error_data_path, "w") as f :
                json.dump(error_data, f)

        print(f"\x1b[92mGenerated sensor data: {file_name}\x1b[0m")
        time.sleep(generate_every)
