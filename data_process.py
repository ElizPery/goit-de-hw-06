from pyspark.sql.functions import from_json, col, to_timestamp, avg, window, to_json, struct, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import SparkSession
from configs import kafka_config
import os

# Package to read Kafka from Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Create SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

# Read data from Kafka
# maxOffsetsPerTrigger - would read 15 messages for 1 triger.
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "eli_building_sensors_1") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "15") \
    .load()

# Create schema for Kafka data
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature_val", IntegerType()),
    StructField("humidity_val", IntegerType()),
    StructField("timestamp", StringType())
])

ROBUST_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"

# Manipulation with data
parsed_df = (df.select(
    from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*").drop("sensor_id")
    .withColumn("timestamp", to_timestamp(col("timestamp"), ROBUST_FORMAT)))

df_agg = (parsed_df.withWatermark("timestamp", "10 seconds")
    .select("temperature_val", "humidity_val", "timestamp")
    .groupBy(window(col("timestamp"), "1 minutes", "30 seconds"))
    .agg(
        avg("temperature_val").alias("t_avg"),
        avg("humidity_val").alias("h_avg")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "t_avg",
        "h_avg"
    ))

# Create schema for CSV data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", IntegerType(), True),
    StructField("humidity_max", IntegerType(), True),
    StructField("temperature_min", IntegerType(), True),
    StructField("temperature_max", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True)
])

# Read streaming data from CSV
csvDF = spark.read.csv("../data/alerts_conditions.csv", header=True)

# Cross join and Filter according to values
filtered_t_df = (df_agg.join(csvDF, how="cross")
    .filter(
        ((col("t_avg") > col("temperature_min"))
        & (col("t_avg") < col("temperature_max"))))
    .drop(*["id", "humidity_min", "humidity_max", "temperature_min", "temperature_max"]))

filtered_h_df = (df_agg.join(csvDF, how="cross")
    .filter(
        ((col("h_avg") > col("humidity_min"))
        & (col("h_avg") < col("humidity_max"))))
    .drop(*["id", "humidity_min", "humidity_max", "temperature_min", "temperature_max"]))

# Prepare data for Kafka: create key-value
prepare_to_kafka_t_df = filtered_t_df.select(
    col("window_start").cast("string").alias("key"),

    # Pack the payload into the 'value' column as JSON
    to_json(struct(
        col("window_start"),
        col("window_end"),
        col("t_avg"),
        col("h_avg"),
        col("code"),
        col("message")
    )).alias("value"))

prepare_to_kafka_h_df = filtered_h_df.select(
    col("window_start").cast("string").alias("key"),

    # Pack the payload into the 'value' column as JSON
    to_json(struct(
        col("window_start"),
        col("window_end"),
        col("t_avg"),
        col("h_avg"),
        col("code"),
        col("message")
    )).alias("value"))

# Write processed data into Kafka-topic 'eli_temperature_alerts' or 'eli_humidity_alerts'

queries = []

temp_query = prepare_to_kafka_t_df.writeStream \
    .trigger(processingTime='5 seconds') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "eli_temperature_alerts_1") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "/tmp/checkpoints-3") \
    .start()
print("tem")

queries.append(temp_query)

humidity_query = prepare_to_kafka_h_df.writeStream \
    .trigger(processingTime='5 seconds') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "eli_humidity_alerts_1") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "/tmp/checkpoints-4") \
    .start()
print("hum")

queries.append(humidity_query)

if queries:
    for query in queries:
        query.awaitTermination()
