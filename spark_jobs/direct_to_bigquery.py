from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from google.cloud import pubsub_v1

spark = SparkSession.builder \
    .appName("Telecom CDR Daily") \
    .config("temporaryGcsBucket", "telecom-cdr-data") \
    .getOrCreate()

df = spark.read.csv("gs://telecom-cdr-data/sample_cdrs.csv", header=True, inferSchema=True)

final = df.groupBy("user_id").agg(
    _sum("duration_min").alias("total_minutes"),
    _sum("data_gb").alias("total_gb")
)

final.write \
    .format("bigquery") \
    .option("table", "cdr_dataset.daily_summary") \
    .mode("overwrite") \
    .save()

print("Data written to BigQuery!")

# REAL-TIME ALERT IF > 2 GB
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("telecom-cdr", "high-usage-alerts")

for row in final.collect():
    if row.total_gb > 2:
        msg = f"User {row.user_id} used {row.total_gb:.2f} GB – HIGH USAGE ALERT!"
        publisher.publish(topic_path, msg.encode("utf-8"))
        print(f"ALERT SENT: {msg}")

print("Daily job + real-time alerts completed!")
spark.stop()
