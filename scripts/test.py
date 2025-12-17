from pathlib import Path

BRONZE_KAFKA_PATH = Path.cwd().parent / "data" / "out" / "delta_bronze_kafka"

print(BRONZE_KAFKA_PATH)