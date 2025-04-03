import threading
import time
from generate_valid import generate_sensor_data
from validator import validate

if __name__ == "__main__":
    threading.Thread(target=generate_sensor_data, daemon=True).start()
    threading.Thread(target=validate, daemon=True).start()

    while True:
        time.sleep(1)