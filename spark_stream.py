from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Create Spark session
spark = (
    SparkSession
    .builder
    .appName("KafkaEmojiAggregation")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .getOrCreate()
)

# Define the schema of the JSON data
json_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", StringType(), True)  
])

# Read data from Kafka
kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")  
    .option("subscribe", "emoji-topic")  
    .option("startingOffsets", "earliest")
    .load()
)

# Cast Kafka message value to String and parse the JSON
kafka_json_df = kafka_df.withColumn("value", col("value").cast("string"))
parsed_df = kafka_json_df.withColumn("jsonData", from_json(col("value"), json_schema)).select("jsonData.*")

# Convert the string to timestamp 
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Perform aggregation on emoji_type 
aggregated_df = (
    parsed_df
    .withWatermark("timestamp", "2 seconds")  
    .groupBy(
        window(col("timestamp"), "2 seconds"),  
        col("emoji_type")
    )
    .agg(count("emoji_type").alias("emoji_count"))  
)

# Convert to JSON
output_df = aggregated_df.selectExpr("cast(window.start as string) as key", "to_json(struct(*)) as value")

# Write stream to Kafka
query = (
    output_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")  
    .option("topic", "emoji-aggregated") 
    .option("checkpointLocation", "/tmp/checkpoints")  
    .outputMode("append")  
    .trigger(processingTime="2 seconds")  
    .start()
)

query.awaitTermination()

#/usr/local/kafka/bin/kafka-topics.sh --create --topic emoji-topic --bootstrap-server localhost:9092

#/usr/local/kafka/bin/kafka-topics.sh --create --topic emoji-aggregated --bootstrap-server localhost:9092
