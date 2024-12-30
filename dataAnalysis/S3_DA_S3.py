from pyspark.sql import SparkSession
import boto3
import time
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType, DoubleType
from pyspark.sql.functions import col, from_json, explode

RAW_DATA_BUCKET = "aws-s3-bucket-fastcampus"
UNPROCESSED_PREFIX = "ClickStream/unprocessed/"
PROCESSED_PREFIX = "ClickStream/processed/"
RESULTS_PATH = f"s3a://{RAW_DATA_BUCKET}/ClickStream/analytics_results/statistics.parquet"
COIN_SELECTION_STATS_PATH = f"s3a://{RAW_DATA_BUCKET}/ClickStream/analytics_results/coin_selection_statistics.parquet"


spark = SparkSession.builder \
    .appName("ClickStreamAnalysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .config("spark.driver.memory", "3g") \
    .config("spark.master", "yarn") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir(f"s3a://{RAW_DATA_BUCKET}/ClickStream/checkpoints")

s3_client = boto3.client('s3')

schema = StructType([
    StructField("state", StructType([
        StructField("coins", ArrayType(StringType()), True)
    ]), True)
])

def list_unprocessed_files(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', [])] if 'Contents' in response else []

def move_to_processed(bucket, file_key):
    processed_key = file_key.replace(UNPROCESSED_PREFIX, PROCESSED_PREFIX)
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': file_key},
        Key=processed_key
    )
    s3_client.delete_object(Bucket=bucket, Key=file_key)
    print(f"Moved {file_key} to {processed_key}")

def analyze_coin_selection_ratio(spark, bucket, file_key):
    s3_path = f"s3a://{bucket}/{file_key}"
    print(f"Reading data from {s3_path}")

    new_df = spark.read.schema(schema).parquet(s3_path)

    coin_counts = new_df.withColumn("coin", explode(col("state.coins"))) \
                        .groupBy("coin") \
                        .count() \
                        .withColumnRenamed("count", "new_count")

    try:
        existing_stats = spark.read.parquet(COIN_SELECTION_STATS_PATH)
        print(f"Loaded existing coin selection statistics from {COIN_SELECTION_STATS_PATH}")
    except Exception as e:
        print("No existing statistics found, starting fresh.")
        existing_stats = spark.createDataFrame([], schema=coin_counts.schema)
    
    if "selection_ratio" not in coin_counts.columns:
        coin_counts = coin_counts.withColumn("selection_ratio", lit(0.0))

    updated_stats = existing_stats.union(coin_counts) \
        .groupBy("coin").sum("new_count") \
        .withColumnRenamed("sum(new_count)", "total_count")

    total_selections = updated_stats.agg({"total_count": "sum"}).collect()[0][0]

    selection_ratio = updated_stats.withColumn(
        "selection_ratio",
        (updated_stats["total_count"] / total_selections * 100)
    )

    selection_ratio.write.mode("overwrite").parquet(COIN_SELECTION_STATS_PATH)
    print(f"Coin selection statistics saved to {COIN_SELECTION_STATS_PATH}")

    return selection_ratio

def main():
    while True:
        unprocessed_files = list_unprocessed_files(RAW_DATA_BUCKET, UNPROCESSED_PREFIX)

        if not unprocessed_files:
            print("No unprocessed files found.")
        else:
            for file_key in unprocessed_files:
                try:
                    analyze_coin_selection_ratio(spark, RAW_DATA_BUCKET, file_key)
                    move_to_processed(RAW_DATA_BUCKET, file_key)
                except Exception as e:
                    print(f"Error processing {file_key}: {e}")

        time.sleep(15)

if __name__ == "__main__":
    main()
