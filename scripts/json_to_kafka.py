import json
import time
import os
from pathlib import Path
from kafka import KafkaProducer

# IMPORTANT : this scripts must be run AFTER the docker images Kafka & Zookeeper (docker-compose up -d)

# --- CONFIGURATION ---
TOPIC_NAME = "sensor-data"
INPUT_FOLDER = Path(__file__).resolve().parent.parent / "data" / "sensor_data"

#  Connexion to Kafka Cluster
print("Connexion to Kafka...")
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],  # kafka server address in docker
    acks="all",
    retries=5,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),  # json to bytes
)
print("Connected !")

# load all JSON files from the input folder
files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.endswith(".json")])
print(f"Sending {len(files)} messages...")

# loop over files and send data to Kafka
try:
    for file_name in files:
        file_path = INPUT_FOLDER / file_name

        # read json file
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue  # skip empty lines
                data = json.loads(line)
                # --- KAFKA---
                # the json is sent as a message to the Kafka topic
                producer.send(TOPIC_NAME, value=data)
                time.sleep(1)
                print(f"Sent : {data.get('device_id')} (File: {file_name})")
            # flush the producer to ensure all messages are sent
            producer.flush()

        # Pause to simulate real-time data streaming
        time.sleep(1)

except KeyboardInterrupt:
    print("Manual stop of thescript.")
finally:
    producer.close()
    print("Closed connection to Kafka.")
