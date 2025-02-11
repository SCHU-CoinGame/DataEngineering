from kafka import KafkaConsumer, TopicPartition
import redis
import json
import threading
from concurrent.futures import ThreadPoolExecutor
import time

# Redis 설정
redis_host = "coincache-ek5phj.serverless.apn2.cache.amazonaws.com"
redis_port = 6379  # Redis 기본 포트
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

# 설정
BATCH_SIZE = 10  # 배치 크기
THREADS = 15  # 병렬 처리 스레드 수
LOCKS = {}  # 파티션별 Lock 관리
PARTITION_LIST = list(range(10))  # 파티션 리스트 (10개)

# **선정한 9개 코인 목록**
coin_codes = ['KRW-BTC', 'KRW-ETH', 'KRW-DOGE', 'KRW-BIGTIME', 'KRW-SUI', 'KRW-UXLINK', 'KRW-SOL', 'KRW-XRP', 'KRW-SXP']

# Redis 캐싱 함수
def write_batch_to_redis(records):
    pipeline = redis_client.pipeline()  # Redis 파이프라인 사용 (일괄 처리 속도 향상)
    for record in records:
        # **필터링: 선택한 코인만 Redis에 적재**
        if record['code'] not in coin_codes:
            continue  # 선정되지 않은 코인은 무시

        key = f"coin:{record['code']}"  # Redis 키 생성 (코인 코드 기반)
        value = {
            'timestamp': record['timestamp'],
            'high_price': record['high_price'],
            'low_price': record['low_price'],
            'trade_price': record['trade_price'],
            'change': record['change'],
            'change_price': record['change_price'],
            'change_rate': record['change_rate'],
        }

        # Redis에 데이터 저장 (Hash 구조 사용)
        pipeline.hset(key, mapping=value)
        pipeline.expire(key, 60)  # TTL 설정 (60초 후 만료)

    try:
        pipeline.execute()  # Redis 파이프라인 실행
        print(f"[INFO] Successfully cached {len(records)} records to Redis.")
    except redis.RedisError as e:
        print(f"[ERROR] Redis pipeline execution failed: {e}")

# 병렬 처리 함수
def process_records_parallel(records):
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            executor.submit(write_batch_to_redis, batch)

# 파티션별 메시지 처리
def process_partition(partition_id):
    # Kafka Consumer 생성 (파티션별)
    consumer = KafkaConsumer(
        bootstrap_servers=['spark-worker-panda-01:9092',
                           'spark-worker-panda-02:9092',
                           'spark-worker-panda-03:9092',
                           'spark-worker-panda-04:9092'],
        enable_auto_commit=False,
        group_id='upbit-consumer-group',  # 그룹 ID 추가
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # 특정 파티션 할당
    tp = TopicPartition('upbit-ticker-data', partition_id)
    consumer.assign([tp])

    buffer = []
    total_count = 0

    try:
        for message in consumer:
            data = message.value

            # **필터링: 선택된 코인만 추가**
            if data['code'] in coin_codes:
                buffer.append(data)

            # 고정 배치 크기 처리
            if len(buffer) >= BATCH_SIZE:
                with LOCKS[partition_id]:  # 파티션별 Lock
                    process_records_parallel(buffer)
                    consumer.commit()  # 성공 시 커밋
                    total_count += len(buffer)
                    buffer.clear()

    except Exception as e:
        print(f"[ERROR] Partition {partition_id} Error: {e}")
    finally:
        # 남은 데이터 처리
        if buffer:
            process_records_parallel(buffer)
            consumer.commit()
        consumer.close()
        print(f"[INFO] Partition {partition_id} processed {total_count} records.")

# 멀티스레드로 병렬 처리 시작
def main():
    threads = []
    # 파티션별 Lock 초기화
    for p in PARTITION_LIST:
        LOCKS[p] = threading.Lock()

    # 각 파티션에 대해 스레드 생성
    for partition_id in PARTITION_LIST:
        thread = threading.Thread(target=process_partition, args=(partition_id,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    print("[INFO] Starting Kafka Consumer with optimized multi-threading and Redis caching...")
    main()
