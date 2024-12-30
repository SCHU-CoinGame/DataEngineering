from pyspark.sql import SparkSession
import boto3
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DoubleType
from pyspark.sql.functions import col, from_json, explode, lit

# S3 bucket and paths
RAW_DATA_BUCKET = "aws-s3-bucket-fastcampus"
UNPROCESSED_PREFIX = "ClickStream/unprocessed/"
PROCESSED_PREFIX = "ClickStream/processed/"
COIN_SELECTION_STATS_DIR = f"s3a://{RAW_DATA_BUCKET}/ClickStream/analytics_results/coin_ratio/"
TEMP_OUTPUT = f"s3a://{RAW_DATA_BUCKET}/ClickStream/analytics_results/coin_ratio/temp_csv"
FINAL_STATS_FILE = "ClickStream/analytics_results/coin_ratio/coin_selection_statistics.csv"

# Spark Session
spark = SparkSession.builder \
    .appName("ClickStream Analysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.master", "local") \
    .getOrCreate()

spark.catalog.clearCache()

# Schema definitions
schema = StructType([
    StructField("userName", StringType(), True),
    StructField("coins", StringType(), True),
])

stats_schema = StructType([
    StructField("coin", StringType(), True),
    StructField("total_count", LongType(), True),
    StructField("selection_ratio", DoubleType(), True)
])


# List unprocessed files
def list_unprocessed_files(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [f"s3a://{bucket}/{obj['Key']}" for obj in response.get('Contents', [])] if 'Contents' in response else []


# Analyze coin selection ratio
def analyze_coin_selection_ratio(spark, s3_path):
    print(f"[INFO] Processing file: {s3_path}")

    # Load new data
    df = spark.read.schema(schema).parquet(s3_path)
    print("[INFO] Loaded data schema:")
    df.printSchema()

    # Parse coins and count
    df = df.withColumn("coins", from_json(col("coins"), ArrayType(StringType())))
    coin_counts = df.withColumn("coin", explode(col("coins"))) \
                    .groupBy("coin") \
                    .count() \
                    .withColumnRenamed("count", "new_count")

    print("[INFO] Coin counts:")
    coin_counts.show(truncate=False)

    # Load existing statistics
    try:
        existing_stats = spark.read.schema(stats_schema).csv(COIN_SELECTION_STATS_DIR + "coin_selection_statistics.csv", header=True)
        print("[INFO] Loaded existing statistics")
        existing_stats.show(truncate=False)

        # Merge with new data
        updated_stats = existing_stats.join(coin_counts, ["coin"], "outer").fillna(0)
        updated_stats = updated_stats.withColumn("total_count", col("total_count") + col("new_count")).drop("new_count")

        # Calculate selection ratios
        total_selections = updated_stats.agg({"total_count": "sum"}).collect()[0][0]
        updated_stats = updated_stats.withColumn(
            "selection_ratio",
            (updated_stats["total_count"] / total_selections * 100)
        )
    except Exception as e:
        print(f"[WARN] No existing statistics found. Error: {e}")
        updated_stats = coin_counts.withColumnRenamed("new_count", "total_count")
        updated_stats = updated_stats.withColumn("selection_ratio", lit(100.0))

    # Final statistics
    print("[INFO] Final stats before saving:")
    updated_stats.show(truncate=False)

    # Save as a temporary CSV
    updated_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(TEMP_OUTPUT)
    print("[INFO] Temporary statistics saved.")

    # Move file to final location
    s3_client = boto3.client('s3')
    temp_prefix = "ClickStream/analytics_results/coin_ratio/temp_csv/"
    bucket = RAW_DATA_BUCKET

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=temp_prefix)
    for obj in response.get('Contents', []):
        if obj['Key'].endswith(".csv"):
            copy_source = {'Bucket': bucket, 'Key': obj['Key']}
            s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=FINAL_STATS_FILE)
            s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
            print(f"[INFO] Moved file {obj['Key']} -> {FINAL_STATS_FILE}")

    print("[INFO] Statistics saved successfully.")


# Process all unprocessed files
files = list_unprocessed_files(RAW_DATA_BUCKET, UNPROCESSED_PREFIX)
print(f"[INFO] Unprocessed files: {files}")

for file_path in files:
    analyze_coin_selection_ratio(spark, file_path)

# Stop Spark session
spark.stop()