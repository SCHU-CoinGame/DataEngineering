from pyspark.sql import SparkSession
import boto3
import json
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, explode, lit, avg, sum as _sum, udf

# 게임 총 시간 (초)
GAME_TIME = 45

# S3 bucket and paths
RAW_DATA_BUCKET = "aws-s3-bucket-fastcampus"
UNPROCESSED_PREFIX = "ClickStream/unprocessed/"
STATS_DIR = f"s3a://{RAW_DATA_BUCKET}/ClickStream/analytics_results/"

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
    StructField("remainingTime_1", LongType(), True),
    StructField("remainingTime_2", LongType(), True),
    StructField("remainingTime_3", LongType(), True),
    StructField("userSellTime_1", StringType(), True),
    StructField("userSellTime_2", StringType(), True),
    StructField("userSellTime_3", StringType(), True),
    StructField("leverage", LongType(), True),
    StructField("aiRecommend_1", BooleanType(), True),
    StructField("aiRecommend_2", BooleanType(), True)
])

def list_unprocessed_files(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    # 파일 경로 리스트 생성
    return [f"s3a://{bucket}/{obj['Key']}" for obj in response.get('Contents', [])] if 'Contents' in response else []

# JSON에서 "time" 추출 함수
def extract_time(data):
    try:
        return json.loads(data).get("time", None) if data else None
    except:
        return None

extract_time_udf = udf(extract_time, LongType())

# 기존 통계와 새로운 통계 병합 및 계산 함수
def merge_statistics(existing_stats, new_stats, key_columns, agg_columns):
    if not existing_stats:
        return new_stats

    merged = existing_stats.join(new_stats, key_columns, "outer").fillna(0)

    for col_name in agg_columns:
        if "avg" in col_name:
            # 평균 통계 병합
            merged = merged.withColumn(
                col_name,
                ((col(col_name + "_x") + col(col_name + "_y")) / 2)
            )
        else:
            # 일반 합산 통계 병합
            merged = merged.withColumn(
                col_name, col(col_name + "_x") + col(col_name + "_y")
            )

        merged = merged.drop(col_name + "_x", col_name + "_y")

    return merged

# S3 파일 처리 함수
def save_to_s3(path, updated_stats):
    updated_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)


# 1. 코인 선택 통계
def analyze_coin_selection(df):
    print("[INFO] Analyzing coin selection statistics...")
    df = df.withColumn("coins", from_json(col("coins"), ArrayType(StringType())))
    coin_counts = df.withColumn("coin", explode(col("coins"))) \
                    .groupBy("coin") \
                    .count() \
                    .withColumnRenamed("count", "total_count")

    total = coin_counts.agg({"total_count": "sum"}).collect()[0][0]
    coin_counts = coin_counts.withColumn("selection_ratio", col("total_count") / total * 100)

    try:
        existing_stats = spark.read.csv(STATS_DIR + "coin_selection_statistics.csv", header=True, inferSchema=True)
        updated_stats = merge_statistics(existing_stats, coin_counts, ["coin"], ["total_count"])
        total_updated = updated_stats.agg({"total_count": "sum"}).collect()[0][0]
        updated_stats = updated_stats.withColumn("selection_ratio", col("total_count") / total_updated * 100)
    except:
        updated_stats = coin_counts

    updated_stats.show(truncate=False)
    save_to_s3(STATS_DIR + "coin_selection_statistics.csv", updated_stats)


# 2. 잔여 시간 통계
def analyze_remaining_time(df):
    print("[INFO] Analyzing remaining time statistics...")
    avg_time = df.select(
        avg("remainingTime_1").alias("avg_time_1"),
        avg("remainingTime_2").alias("avg_time_2"),
        avg("remainingTime_3").alias("avg_time_3")
    )

    try:
        existing_stats = spark.read.csv(STATS_DIR + "remaining_time_statistics.csv", header=True, inferSchema=True)
        updated_stats = merge_statistics(existing_stats, avg_time, [], ["avg_time_1", "avg_time_2", "avg_time_3"])
    except:
        updated_stats = avg_time

    updated_stats.show(truncate=False)
    save_to_s3(STATS_DIR + "remaining_time_statistics.csv", updated_stats)


# 3. 매도 시간 통계
def analyze_sell_time(df):
    for i in range(1, 4):
        df = df.withColumn(f"sell_time_{i}", extract_time_udf(col(f"userSellTime_{i}")))
        df = df.withColumn(f"elapsed_time_{i}", lit(GAME_TIME) - col(f"sell_time_{i}"))

    avg_sell_time = df.select(
        avg("elapsed_time_1").alias("avg_sell_time_1"),
        avg("elapsed_time_2").alias("avg_sell_time_2"),
        avg("elapsed_time_3").alias("avg_sell_time_3")
    )

    try:
        existing_stats = spark.read.csv(STATS_DIR + "sell_time_statistics.csv", header=True, inferSchema=True)
        updated_stats = merge_statistics(existing_stats, avg_sell_time, [], ["avg_sell_time_1", "avg_sell_time_2", "avg_sell_time_3"])
    except:
        updated_stats = avg_sell_time

    save_to_s3(STATS_DIR + "sell_time_statistics.csv", updated_stats)

# 4. 레버리지 선택 통계
def analyze_leverage(df):
    print("[INFO] Analyzing leverage statistics...")

    # 배치 통계 계산
    leverage_counts = df.groupBy("leverage").count()
    total = leverage_counts.agg({"count": "sum"}).collect()[0][0]
    leverage_stats = leverage_counts.withColumn("selection_ratio", col("count") / total * 100)

    print("[INFO] Leverage Selection Statistics (Current Batch):")
    leverage_stats.show(truncate=False)

    # 기존 통계 불러오기
    try:
        existing_stats = spark.read.csv(STATS_DIR + "leverage_statistics.csv", header=True, inferSchema=True)
        print("[INFO] Existing statistics loaded:")
        existing_stats.show(truncate=False)

        # 기존 통계와 새로운 통계 병합
        updated_stats = existing_stats.join(leverage_stats, ["leverage"], "outer").fillna(0)
        updated_stats = updated_stats.withColumn("count", col("count") + col("count_1")).drop("count_1")

        # 새로운 비율 재계산
        total_leverage = updated_stats.agg({"count": "sum"}).collect()[0][0]
        updated_stats = updated_stats.withColumn("selection_ratio", col("count") / total_leverage * 100)
    except Exception as e:
        print(f"[WARN] No existing statistics found. Error: {e}")
        updated_stats = leverage_stats

    # 결과 저장
    save_to_s3(STATS_DIR + "leverage_statistics.csv", updated_stats)


# 5. AI 추천 통계
def analyze_ai_recommend(df):
    print("[INFO] Analyzing AI recommendation statistics...")

    # 배치 통계 계산
    ai_stats = df.selectExpr(
        "sum(case when aiRecommend_1 = true then 1 else 0 end) as ai_1_true",
        "sum(case when aiRecommend_2 = true then 1 else 0 end) as ai_2_true",
        "count(aiRecommend_1) as total_1",
        "count(aiRecommend_2) as total_2"
    ).withColumn(
        "ai_recommend_ratio_1", (col("ai_1_true") / col("total_1") * 100).cast(DoubleType())
    ).withColumn(
        "ai_recommend_ratio_2", (col("ai_2_true") / col("total_2") * 100).cast(DoubleType())
    )

    print("[INFO] AI Recommendation Statistics (Current Batch):")
    ai_stats.show(truncate=False)

    # 기존 통계 불러오기
    try:
        existing_stats = spark.read.csv(STATS_DIR + "ai_recommend_statistics.csv", header=True, inferSchema=True)
        print("[INFO] Existing statistics loaded:")
        existing_stats.show(truncate=False)

        # 기존 통계와 새로운 통계 병합
        updated_stats = ai_stats.crossJoin(existing_stats)

        # 누적 통계 계산
        updated_stats = updated_stats.select(
            (col("ai_1_true") + col("ai_1_true_1")).alias("ai_1_true"),
            (col("total_1") + col("total_1_1")).alias("total_1"),
            ((col("ai_1_true") + col("ai_1_true_1")) / (col("total_1") + col("total_1_1")) * 100).alias(
                "ai_recommend_ratio_1"),
            (col("ai_2_true") + col("ai_2_true_1")).alias("ai_2_true"),
            (col("total_2") + col("total_2_1")).alias("total_2"),
            ((col("ai_2_true") + col("ai_2_true_1")) / (col("total_2") + col("total_2_1")) * 100).alias(
                "ai_recommend_ratio_2")
        )
    except Exception as e:
        print(f"[WARN] No existing statistics found. Error: {e}")
        updated_stats = ai_stats

    # 결과 저장
    save_to_s3(STATS_DIR + "ai_recommend_statistics.csv", updated_stats)


files = list_unprocessed_files(RAW_DATA_BUCKET, UNPROCESSED_PREFIX)
print(f"[INFO] Unprocessed files: {files}")

# 파일별 통계 처리
for file_path in files:
    print(f"[INFO] Processing file: {file_path}")
    df = spark.read.schema(schema).parquet(file_path)

    # 통계 분석 수행
    analyze_coin_selection(df)
    analyze_remaining_time(df)
    analyze_leverage(df)
    analyze_ai_recommend(df)

spark.stop()
