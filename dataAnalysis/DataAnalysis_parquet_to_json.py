from pyspark.sql import SparkSession
import boto3
import time
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, explode, avg, count, lit, unix_timestamp, when, isnull, round

AWS_REGION = os.getenv("AWS_REGION")
RAW_DATA_BUCKET = os.getenv("RAW_DATA_BUCKET")
UNPROCESSED_PREFIX = os.getenv("UNPROCESSED_PREFIX")
RESULTS_PATH = os.getenv("RESULTS_PATH")

spark = SparkSession.builder \
    .appName("ClickStream Analysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.master", "local[*]").getOrCreate()

spark.sparkContext.setCheckpointDir(f"s3a://{RAW_DATA_BUCKET}/ClickStream/checkpoints")

s3_client = boto3.client('s3', region_name=AWS_REGION)

# 스키마 정의
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
    StructField("aiRecommend_2", BooleanType(), True),
    StructField("balance", DoubleType(), True)
])

def save_to_s3_as_json(path, updated_stats):
    print(f"[INFO] Saving stats to {path} in JSON format...")
    temp_output = path + "_temp"

    updated_stats.coalesce(1).write.mode("overwrite").json(temp_output)

    bucket = RAW_DATA_BUCKET
    prefix = temp_output.replace(f"s3a://{RAW_DATA_BUCKET}/", "")
    final_file = path.replace(f"s3a://{RAW_DATA_BUCKET}/", "")

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []):
        if obj['Key'].endswith(".json"):
            copy_source = {'Bucket': bucket, 'Key': obj['Key']}
            s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=final_file)
            print(f"[INFO] Copied file {obj['Key']} -> {final_file}")

    print("[INFO] Statistics saved successfully.")



# S3 경로의 파일 목록 가져오기
def list_unprocessed_files(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', [])] if 'Contents' in response else []

#코인 선택 비율 통계
def analyze_coin_selection_ratio(df):
    print("[INFO] Analyzing coin selection ratio...")

    exploded_df = df.withColumn("coin", explode(from_json(col("coins"), ArrayType(StringType()))))
    coin_counts = exploded_df.groupBy("coin").agg(count("*").alias("count"))
    total_count = coin_counts.agg({"count": "sum"}).collect()[0][0]
    # print(f"[INFO] Total coin selection count: {total_count}")
    coin_stats = coin_counts.withColumn("ratio", (col("count") / total_count * 100))
    # coin_stats.show(truncate=False)
    save_to_s3_as_json(RESULTS_PATH + "coin_ratio.json", coin_stats)

#페이지 별 남은 사용 시간 통계
def analyze_remaining_time_avg(df):
    print("[INFO] Analyzing average remaining time...")

    avg_times = df.select(
        avg("remainingTime_1").alias("avg_time_1"),
        avg("remainingTime_2").alias("avg_time_2"),
        avg("remainingTime_3").alias("avg_time_3")
    )
    save_to_s3_as_json(RESULTS_PATH + "page_time_avg.json", avg_times)
    # print("[INFO] Remaining time average analysis completed and saved.")

json_schema = StructType([
    StructField("coin", StringType(), True),
    StructField("time", IntegerType(), True)
])

#매도 시간 별 평균 통계
def analyze_sell_time_avg(df):
    print("[INFO] Analyzing average sell time...")

    df = df.withColumn("sell_time_1", from_json(col("userSellTime_1"), json_schema).getField("time")) \
           .withColumn("sell_time_2", from_json(col("userSellTime_2"), json_schema).getField("time")) \
           .withColumn("sell_time_3", from_json(col("userSellTime_3"), json_schema).getField("time"))

    avg_sell_times = df.select(
        avg("sell_time_1").alias("avg_sell_time_1"),
        avg("sell_time_2").alias("avg_sell_time_2"),
        avg("sell_time_3").alias("avg_sell_time_3")
    )

    # avg_sell_times.show(truncate=False)

    save_to_s3_as_json(RESULTS_PATH + "sell_time_avg.json", avg_sell_times)
    # print("[INFO] Sell time average analysis completed and saved.")

# 코인 별 매도 시간 평균 통계
def analyze_coin_avg_sell_time(df):
    print("[INFO] Analyzing average sell time by coin...")

    json_schema = StructType([
        StructField("coin", StringType(), True),
        StructField("time", IntegerType(), True)
    ])

    sell_df = df.select(
        from_json(col("userSellTime_1"), json_schema).alias("sell_1"),
        from_json(col("userSellTime_2"), json_schema).alias("sell_2"),
        from_json(col("userSellTime_3"), json_schema).alias("sell_3")
    )

    exploded_df = sell_df.select(
        col("sell_1.coin").alias("coin"), col("sell_1.time").alias("time")
    ).union(
        sell_df.select(col("sell_2.coin").alias("coin"), col("sell_2.time").alias("time"))
    ).union(
        sell_df.select(col("sell_3.coin").alias("coin"), col("sell_3.time").alias("time"))
    ).filter(col("coin").isNotNull())

    coin_avg_sell_times = exploded_df.groupBy("coin").agg(
        avg("time").alias("avg_sell_time")
    )

    # coin_avg_sell_times.show(truncate=False)
    save_to_s3_as_json(RESULTS_PATH + "coin_avg_sell_time.json", coin_avg_sell_times)
    # print("[INFO] Coin average sell time analysis completed and saved.")

# 코인 별 매도 시간 평균 상위 10%
def analyze_top_10_percent_coin_avg_sell_time(df):
    print("[INFO] Analyzing top 10% average sell time by coin")

    quantile = 0.9
    balance_threshold = df.approxQuantile("balance", [quantile], 0.0)[0]
    top_10_df = df.filter(col("balance") >= balance_threshold)

    json_schema = StructType([
        StructField("coin", StringType(), True),
        StructField("time", IntegerType(), True)
    ])

    sell_df = top_10_df.select(
        from_json(col("userSellTime_1"), json_schema).alias("sell_1"),
        from_json(col("userSellTime_2"), json_schema).alias("sell_2"),
        from_json(col("userSellTime_3"), json_schema).alias("sell_3")
    )

    exploded_df = sell_df.select(
        col("sell_1.coin").alias("coin"), col("sell_1.time").alias("time")
    ).union(
        sell_df.select(col("sell_2.coin").alias("coin"), col("sell_2.time").alias("time"))
    ).union(
        sell_df.select(col("sell_3.coin").alias("coin"), col("sell_3.time").alias("time"))
    ).filter(col("coin").isNotNull())

    coin_avg_sell_times = exploded_df.groupBy("coin").agg(avg("time").alias("avg_sell_time"))
    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_coin_avg_sell_time.json", coin_avg_sell_times)


# 레버리지 비율 통계
def analyze_leverage_ratio(df):
    print("[INFO] Analyzing leverage ratio...")

    leverage_counts = df.groupBy("leverage").agg(count("*").alias("count"))

    total_count = leverage_counts.agg({"count": "sum"}).collect()[0][0]
    # print(f"[INFO] Total leverage selection count: {total_count}")

    leverage_stats = leverage_counts.withColumn("ratio", (col("count") / total_count * 100))
    # leverage_stats.show(truncate=False)

    save_to_s3_as_json(RESULTS_PATH + "leverage_ratio.json", leverage_stats)
    # print("[INFO] Leverage ratio analysis completed and saved.")

# 레버리지 비율 상위 10%
def analyze_top_10_percent_leverage_ratio(df):
    print("[INFO] Analyzing top 10% leverage ratio")

    quantile = 0.9
    balance_threshold = df.approxQuantile("balance", [quantile], 0.0)[0]
    top_10_df = df.filter(col("balance") >= balance_threshold)

    leverage_counts = top_10_df.groupBy("leverage").agg(count("*").alias("count"))

    total_count = leverage_counts.agg({"count": "sum"}).collect()[0][0]
    leverage_stats = leverage_counts.withColumn("ratio", (col("count") / total_count * 100))

    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_leverage_ratio.json", leverage_stats)

# 레버리지 별 평균 자산
def analyze_leverage_avg_balance(df):
    print("[INFO] Analyzing leverage avg balance relationship...")

    leverage_stats = df.groupBy("leverage").agg(
        avg("balance").alias("avg_balance")  # 평균 잔고 계산
    )
    save_to_s3_as_json(RESULTS_PATH + "leverage_avg_balance.json", leverage_stats)
    # print("[INFO] Leverage average balance analysis completed and saved.")

# 레버리지 별 평균 자산 상위 10%
def analyze_top_10_percent_leverage_avg_balance(df):
    print("[INFO] Analyzing top 10% leverage avg balance relationship")

    quantile = 0.9
    balance_threshold = df.approxQuantile("balance", [quantile], 0.0)[0]
    top_10_df = df.filter(col("balance") >= balance_threshold)

    leverage_stats = top_10_df.groupBy("leverage").agg(avg("balance").alias("avg_balance"))

    # 결과 저장
    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_leverage_avg_balance.json", leverage_stats)


# ai 추천 기능 사용 비율 통계
def analyze_ai_recommend_ratio(df):
    print("[INFO] Analyzing AI recommend usage ratio...")

    ai1_counts = df.groupBy("aiRecommend_1").agg(count("*").alias("count"))
    total_ai1 = ai1_counts.agg({"count": "sum"}).collect()[0][0]
    ai1_stats = ai1_counts.withColumn("ratio", (col("count") / total_ai1 * 100))

    ai2_counts = df.groupBy("aiRecommend_2").agg(count("*").alias("count"))
    total_ai2 = ai2_counts.agg({"count": "sum"}).collect()[0][0]
    ai2_stats = ai2_counts.withColumn("ratio", (col("count") / total_ai2 * 100))

    save_to_s3_as_json(RESULTS_PATH + "ai_recommend_1_ratio.json", ai1_stats)
    save_to_s3_as_json(RESULTS_PATH + "ai_recommend_2_ratio.json", ai2_stats)

    # print("[INFO] AI recommend ratio analysis completed and saved.")

# ai 추천 기능 사용 비율 상위 10%
def analyze_top_10_percent_ai_recommend_ratio(df):
    print("[INFO] Analyzing top 10% AI recommend usage ratio")

    quantile = 0.9
    balance_threshold = df.approxQuantile("balance", [quantile], 0.0)[0]
    top_10_df = df.filter(col("balance") >= balance_threshold)

    ai1_counts = top_10_df.groupBy("aiRecommend_1").agg(count("*").alias("count"))
    total_ai1 = ai1_counts.agg({"count": "sum"}).collect()[0][0]
    ai1_stats = ai1_counts.withColumn("ratio", (col("count") / total_ai1 * 100))

    ai2_counts = top_10_df.groupBy("aiRecommend_2").agg(count("*").alias("count"))
    total_ai2 = ai2_counts.agg({"count": "sum"}).collect()[0][0]
    ai2_stats = ai2_counts.withColumn("ratio", (col("count") / total_ai2 * 100))

    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_ai_recommend_1_ratio.json", ai1_stats)
    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_ai_recommend_2_ratio.json", ai2_stats)


# ai 추천 평균 잔고
def analyze_ai_recommendation_avg_balance(df):
    print("[INFO] Analyzing AI recommendation avg_balance")

    ai1_df = df.groupBy("aiRecommend_1").agg(avg("balance").alias("avg_balance"))

    ai2_df = df.groupBy("aiRecommend_2").agg(avg("balance").alias("avg_balance"))

    save_to_s3_as_json(RESULTS_PATH + "ai_recommend_1_avg_balance.json", ai1_df)
    save_to_s3_as_json(RESULTS_PATH + "ai_recommend_2_avg_balance.json", ai2_df)

    # print("[INFO] AI recommendation performance analysis completed and saved.")

# ai 추천 평균 잔고 상위 10%
def analyze_top_10_percent_ai_recommendation_avg_balance(df):
    print("[INFO] Analyzing top 10% AI recommendation avg_balance")

    quantile = 0.9
    balance_threshold = df.approxQuantile("balance", [quantile], 0.0)[0]
    top_10_df = df.filter(col("balance") >= balance_threshold)

    ai1_df = top_10_df.groupBy("aiRecommend_1").agg(avg("balance").alias("avg_balance"))

    ai2_df = top_10_df.groupBy("aiRecommend_2").agg(avg("balance").alias("avg_balance"))

    # 결과 저장
    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_ai_recommend_1_avg_balance.json", ai1_df)
    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_ai_recommend_2_avg_balance.json", ai2_df)


# 자산 평균
def analyze_balance_statistics(df):
    print("[INFO] Analyzing balance statistics...")

    avg_balance = df.select(avg("balance").alias("avg_balance"))

    save_to_s3_as_json(RESULTS_PATH + "avg_balance.json", avg_balance)
    # print("[INFO] Average balance analysis completed and saved.")

# 상위 잔고 10%의 코인 선택 비율
def analyze_top_10_percent_coin_ratio(df):
    print("[INFO] Analyzing top 10% player coin selection ratio...")

    quantile = 0.9
    balance_threshold = df.approxQuantile("balance", [quantile], 0.0)[0]

    top_10_df = df.filter(col("balance") >= balance_threshold)

    exploded_df = top_10_df.withColumn(
        "coins",
        when(isnull(col("coins")), "[]").otherwise(col("coins"))
    ).withColumn(
        "coin",
        explode(from_json(col("coins"), ArrayType(StringType())))
    )
    coin_counts = exploded_df.groupBy("coin").agg(count("*").alias("count"))

    total_count = coin_counts.agg({"count": "sum"}).collect()[0][0]
    coin_stats = coin_counts.withColumn("ratio", round(col("count") / total_count * 100, 2))
    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_coin_ratio.json", coin_stats)
    # print("[INFO] Top 10% player coin selection ratio analysis completed and saved.")

# 상위 잔고 10% 매도 시간 평균 
def analyze_top_10_percent_avg_sell_time_by_category(df):
    print("[INFO] Analyzing top 10% player average sell time by category...")

    quantile = 0.9
    balance_threshold = df.approxQuantile("balance", [quantile], 0.0)[0]

    top_10_df = df.filter(col("balance") >= balance_threshold)

    json_schema = StructType([
        StructField("coin", StringType(), True),
        StructField("time", IntegerType(), True)
    ])

    sell_df = top_10_df.select(
        from_json(col("userSellTime_1"), json_schema).alias("sell_1"),
        from_json(col("userSellTime_2"), json_schema).alias("sell_2"),
        from_json(col("userSellTime_3"), json_schema).alias("sell_3")
    )

    avg_times = sell_df.select(
        avg(col("sell_1.time")).alias("avg_sell_time_1"),
        avg(col("sell_2.time")).alias("avg_sell_time_2"),
        avg(col("sell_3.time")).alias("avg_sell_time_3")
    )

    save_to_s3_as_json(RESULTS_PATH + "top_10_percent_avg_sell_time_by_category.json", avg_times)
    # print("[INFO] Top 10% player average sell time by category analysis completed and saved.")

# 분석 데이터 실행
def analyze_data(df):
    analyze_coin_selection_ratio(df)
    analyze_remaining_time_avg(df)
    analyze_sell_time_avg(df)
    analyze_coin_avg_sell_time(df)
    analyze_leverage_ratio(df)
    analyze_ai_recommend_ratio(df)
    analyze_balance_statistics(df) 
    analyze_leverage_avg_balance(df)
    analyze_ai_recommendation_avg_balance(df)
    analyze_top_10_percent_coin_ratio(df)
    analyze_top_10_percent_avg_sell_time_by_category(df)
    analyze_top_10_percent_ai_recommendation_avg_balance(df)
    analyze_top_10_percent_ai_recommend_ratio(df)
    analyze_top_10_percent_leverage_avg_balance(df)
    analyze_top_10_percent_leverage_ratio(df)
    analyze_top_10_percent_coin_avg_sell_time(df)

def main():
    while True:
        # print("[INFO] Checking for unprocessed files...")
        unprocessed_files = list_unprocessed_files(RAW_DATA_BUCKET, UNPROCESSED_PREFIX)

        if not unprocessed_files:
            print("[INFO] No unprocessed files found.")
        else:
            all_dataframes = []
            for file_key in unprocessed_files:
                try:
                    s3_path = f"s3a://{RAW_DATA_BUCKET}/{file_key}"
                    # print(f"[INFO] Reading data from {s3_path}")
                    df = spark.read.schema(schema).parquet(s3_path)
                    df = df.withColumn("remainingTime_1", col("remainingTime_1").cast("int")) \
                        .withColumn("remainingTime_2", col("remainingTime_2").cast("int")) \
                        .withColumn("remainingTime_3", col("remainingTime_3"))

                    all_dataframes.append(df)
                except Exception as e:
                    print(f"[ERROR] Failed to process {file_key}: {e}")

            if all_dataframes:
                merged_df = all_dataframes[0]
                for df in all_dataframes[1:]:
                    merged_df = merged_df.union(df)

                analyze_data(merged_df)

        print("[INFO] Sleeping for 2 minutes...")
        time.sleep(86400) #하루 주기


if __name__ == "__main__":
    main()
