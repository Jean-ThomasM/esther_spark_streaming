# This is the streaming pipeline, acting as a consumer of Kafka messages and
# writing them into a Delta table.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    TimestampType,
)
from pathlib import Path
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

kafka_packages = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"

builder = (
    SparkSession.builder.appName("SmartTech_Kafka_to_Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)


spark = configure_spark_with_delta_pip(builder,extra_packages=[kafka_packages]).getOrCreate()

print(f"Spark version : {spark.version}")
print("Kafka and Delta connectors loaded")

# definition of the streaming dataframe schema, with a 'corrupt_reccord' column in case of errors
schema_sensor = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("device_id", StringType(), True),
        StructField("building", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("value", FloatType(), True),
        StructField("unit", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)
# definition of the paths
BRONZE_KAFKA_PATH = Path(__file__).resolve().parent.parent / "data" / "out" / "delta_bronze_kafka"
CHECKPOINT_KAFKA_PATH = Path(__file__).resolve().parent.parent / "data" / "out" / "checkpoint_bronze_kafka"

# definition of the streaming dataframe - lazy execution
df_kafka_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor-data")
    .option("startingOffsets", "earliest")
    .load()
)

json_options = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record",
}

df_parsed = (
    df_kafka_raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(
        from_json(col("json_str"), schema_sensor, options=json_options).alias("data")
    )
    .select("data.*")
)


# we filter the Null values - lazy execution
# df_stream_clean = df_parsed.filter(
#     col("timestamp").isNotNull()
#     & col("device_id").isNotNull()
#     & col("building").isNotNull()
#     & col("floor").isNotNull()
#     & col("type").isNotNull()
#     & col("value").isNotNull()
#     & col("unit").isNotNull()
# )


# writing the clean streaming dataframe into a Delta table
query = (
    df_parsed.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", str(CHECKPOINT_KAFKA_PATH))
    .option("path", str(BRONZE_KAFKA_PATH))
    .trigger(processingTime="1 seconds")
    .toTable("bronze_sensor_data")
)

spark.streams.awaitAnyTermination()