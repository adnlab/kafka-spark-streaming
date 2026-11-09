import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, lit, coalesce, concat_ws,
    row_number, current_timestamp, to_json, struct
)
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = "transactions"
VALID_TOPIC = "transactions_valid"
DLQ_TOPIC = "transactions_dlq"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

seen_keys = set()
total_valid = [0]

def get_spark_session():
    return SparkSession.builder \
        .appName("TransactionsETL") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def get_schema():
    return StructType([StructField(f, StringType(), True)
                       for f in ["user_id", "amount", "timestamp", "source"]])

def read_topic(spark, topic):
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    key = when(col("error_reason") == "", concat_ws("|", "user_id", "timestamp"))
    w = Window.partitionBy("key").orderBy("partition", "offset")
    df = batch_df.withColumn("key", key).withColumn("rn", row_number().over(w))
    is_dup = col("key").isNotNull() & ((col("rn") > 1) | col("key").isin(list(seen_keys)))
    df = df.withColumn("error_reason", when(is_dup, lit("DUPLICATE_EVENT")).otherwise(col("error_reason"))) \
           .withColumn("is_valid", col("error_reason") == "") \
           .persist()

    valid = df.filter("is_valid")
    invalid = df.filter("NOT is_valid")
    seen_keys.update(r.key for r in valid.select("key").collect())

    for data, topic in [(valid, VALID_TOPIC), (invalid, DLQ_TOPIC)]:
        data.select(to_json(struct("user_id", "amount", "timestamp", "source", "is_valid", "error_reason",
                                   col("json_val").alias("raw"))).alias("value")) \
            .write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", topic).save()

    total_valid[0] += valid.count()
    late = invalid.filter(col("error_reason").contains("LATE_EVENT")).count()
    df.sparkSession.range(1).select(
        current_timestamp().alias("timestamp"),
        lit(total_valid[0]).alias("running_total"),
        lit(late).alias("late_in_batch")
    ).show(truncate=False)
    df.unpersist()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    parsed = read_topic(spark, INPUT_TOPIC) \
        .selectExpr("CAST(value AS STRING) AS json_val", "CAST(timestamp AS LONG) AS kafka_time", "partition", "offset") \
        .select(from_json("json_val", get_schema()).alias("d"), "json_val", "kafka_time", "partition", "offset") \
        .select("d.*", "json_val", "kafka_time", "partition", "offset") \
        .withColumn("amount_num", col("amount").cast("double")) \
        .withColumn("event_time", to_timestamp("timestamp", TS_FORMAT))

    validated = parsed.withColumn("error_reason", concat_ws(",",
        when(col("user_id").isNull() | col("amount").isNull() | col("timestamp").isNull(), lit("MISSING_FIELD")),
        when((col("amount").isNotNull() & col("amount_num").isNull()) |
             (col("timestamp").isNotNull() & col("event_time").isNull()), lit("INVALID_TYPE")),
        when(~col("amount_num").between(1, 10_000_000), lit("AMOUNT_OUT_OF_RANGE")),
        when(~coalesce(col("source").isin("mobile", "web", "pos"), lit(False)), lit("INVALID_SOURCE")),
        when(col("kafka_time") - col("event_time").cast("long") > 180, lit("LATE_EVENT"))))

    validated.writeStream.foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/ckpt/main").start()

    window_query = read_topic(spark, VALID_TOPIC) \
        .select(from_json(col("value").cast("string"), "timestamp string").alias("d")) \
        .select(to_timestamp("d.timestamp", TS_FORMAT).alias("event_time")) \
        .withWatermark("event_time", "3 minutes") \
        .groupBy(window("event_time", "1 minute")).count() \
        .writeStream.format("console").outputMode("update").option("truncate", "false") \
        .option("checkpointLocation", "/tmp/ckpt/window").start()

    print("Pipeline berjalan!")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
