from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
from pathlib import Path
from delta import configure_spark_with_delta_pip
import time


# Spark session with Delta Lake support
builder = (
    SparkSession.builder.appName("SmartTech_Bronze_to_Silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define paths
ROOT_FOLDER = Path(__file__).resolve().parent.parent
DELTA_BRONZE_KAFKA_PATH = ROOT_FOLDER / "data" / "out" / "delta_bronze_kafka"
DELTA_SILVER_PATH = ROOT_FOLDER / "data" / "out" / "delta_silver_kafka"
CHECKPOINT_SILVER_KAFKA_PATH = ROOT_FOLDER / "data" / "out" / "checkpoint_silver_kafka"

# wait until the bronze kafka delta table is created
while not DELTA_BRONZE_KAFKA_PATH.exists():
    print("Waiting for bronze Kafka data...")
    time.sleep(5)

print("Bronze Kafka data found. Starting Silver processing...")

df_bronze_kafka = spark.readStream.format("delta").load(str(DELTA_BRONZE_KAFKA_PATH))

cols_texte = ["device_id", "building", "type", "unit"]


# # filter rows where _corrupt_record is not null
#  drop rows with nulls in critial columns
# trim and lower text columns
# drop duplicates
# drop _corrupt_record column
df_silver = (
    df_bronze_kafka.filter(col("_corrupt_record").isNull())
    .dropna(subset=["device_id", "timestamp", "value"])
    .withColumns({c: lower(trim(col(c))) for c in cols_texte})
    .dropDuplicates(["device_id", "timestamp"])
    .drop("_corrupt_record")
)

query = (
    df_silver.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", str(CHECKPOINT_SILVER_KAFKA_PATH))
    .trigger(processingTime="2 seconds")
    .option("path", str(DELTA_SILVER_PATH))
    .start()
)

query.awaitTermination()
