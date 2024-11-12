from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import expr, from_json, col, count, window, floor, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Create Spark Session
spark = (
    SparkSession
    .builder
    .appName("Emoji Aggregation from Kafka")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')
    .config("spark.sql.shuffle.partitions", 4)
    .master("local[*]")
    .getOrCreate()
)

# Read from Kafka topic
kafka_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "ed-kafka:29092")
    .option("subscribe", "emoji-data")
    .option("startingOffsets", "earliest")
    .load()
)

# Cast Kafka 'value' field as string
kafka_json_df = kafka_df.withColumn("value", expr("cast(value as string)"))

# Define schema for the JSON data
json_schema = (
    StructType(
    [StructField('user_id', StringType(), True), 
     StructField('emoji', StringType(), True), 
     StructField('time_stamp', StringType(), True)  # time_stamp will be converted later
    ])
)

# Parse the JSON data from the 'value' column
streaming_df = kafka_json_df.withColumn("values_json", from_json(col("value"), json_schema)).selectExpr("values_json.*")

# Convert 'time_stamp' column to TimestampType
streaming_df = streaming_df.withColumn("time_stamp", to_timestamp(col("time_stamp"), "yyyy-MM-dd HH:mm:ss"))

# Aggregate emojis in 2-second windows
aggregated_df = (
    streaming_df
    .groupBy(
        window(col("time_stamp"), "2 seconds"),  # Aggregate in 2-second intervals
        col("emoji")
    )
    .agg(count("emoji").alias("emoji_count"))  # Count occurrences of each emoji
    .withColumn("aggregated_emoji_count",floor(col("emoji_count") / 1000))  # Aggregate into units of 1000
    .withColumn("aggregated_emoji_count", 
                expr("if(aggregated_emoji_count == 0, 1, aggregated_emoji_count)"))  # Ensure at least 1 if count < 1000
)

# You can send this aggregated result back to Kafka or clients.
output_df = aggregated_df.selectExpr("cast(window.start as string) as key", "to_json(struct(*)) as value")

query = (
    output_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "ed-kafka:29092")
    .option("topic", "emoji-aggregated")
    .option("checkpointLocation", "/tmp/checkpoints")
    .trigger(processingTime="2 seconds")  # Trigger every 2 seconds
    .start()
)

# Await termination to keep the stream running
query.awaitTermination()
