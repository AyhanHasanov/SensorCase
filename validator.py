import os
import json
import time
from datetime import datetime
from aws_helper import upload

upload_dir = "data/upload"
error_dir = "data/error"
stable_dir = "data/stable"
os.makedirs(error_dir, exist_ok=True)
os.makedirs(stable_dir, exist_ok=True)
os.makedirs(upload_dir, exist_ok=True)


def validate() :
    while True :
        time.sleep(60)
        stable_data = []
        error_data = []
        timestamp = datetime.now()

        for file in os.listdir(stable_dir) :
            file_path = os.path.join(stable_dir, file)

            with open(file_path, "r") as f :
                stable_data.extend(json.load(f))

            merged_file_path = f"{upload_dir}/Stable_{timestamp.strftime("%Y%m%d-%H%M%S")}.json"
            with open(merged_file_path, "w") as f :
                json.dump(stable_data, f)

            os.remove(file_path)

        for file in os.listdir(error_dir) :
            file_path = os.path.join(error_dir, file)

            with open(file_path, "r") as f :
                error_data.extend(json.load(f))

            merged_file_path = f"{upload_dir}/Error_{timestamp.strftime("%Y%m%d-%H%M%S")}.json";
            with open(merged_file_path, "w") as f :
                json.dump(error_data, f)

            os.remove(file_path)

        upload()