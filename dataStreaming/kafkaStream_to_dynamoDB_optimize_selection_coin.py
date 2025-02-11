from kafka import KafkaConsumer, TopicPartition
import boto3
import json
import decimal
import threading
from concurrent.futures import ThreadPoolExecutor
import time

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
table = dynamodb.Table('dynamoDB_upbit_table')

BATCH_SIZE = 10
THREADS = 15
LOCKS = {}  # 파티션별 Lock 관리
MAX_RETRIES = 3
PARTITION_LIST = list(range(10))

coin_codes = ['KRW-BTC', 'KRW-ETH', 'KRW-DOGE', 'KRW-BIGTIME', 'KRW-SUI', 'KRW-UXLINK', 'KRW-SOL', 'KRW-XRP', 'KRW-SXP']

# DynamoDB 적재 함수
def write_batch_to_dynamodb(records):
    for record in records:
        if record['code'] not in coin_codes:
            continue

        retries = MAX_RETRIES
        while retries > 0:
            try:
                # 업서트(Upsert) 처리 - 기존 데이터 업데이트 또는 삽입
                table.update_item(
                    Key={
                        'code': record['code'], 
                        'trade_timestamp': int(record['trade_timestamp']) 
                    },
                    UpdateExpression="""
                        SET #ts = :timestamp,
                            high_price = :high_price,
                            low_price = :low_price,
                            trade_price = :trade_price,
                            #ch = :change,
                            change_price = :change_price,
                            change_rate = :change_rate
                    """,
                    ExpressionAttributeNames={
                        '#ts': 'timestamp',  # 예약어 처리
                        '#ch': 'change'      # 예약어 처리
                    },
                    ExpressionAttributeValues={
                        ':timestamp': int(record['timestamp']),
                        ':high_price': decimal.Decimal(str(record['high_price'])),
                        ':low_price': decimal.Decimal(str(record['low_price'])),
                        ':trade_price': decimal.Decimal(str(record['trade_price'])),
                        ':change': record['change'],
                        ':change_price': decimal.Decimal(str(record['change_price'])),
                        ':change_rate': decimal.Decimal(str(record['change_rate']))
                    }
                )
                break  # 성공 시 종료
            except Exception as e:
                retries -= 1
                if retries == 0:  # 최대 재시도 초과
                    print(f"[ERROR] Failed to write record after retries: {e}")
                time.sleep(2 ** (MAX_RETRIES - retries))


# 병렬 처리 함수
def process_records_parallel(records):
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            executor.submit(write_batch_to_dynamodb, batch)

# 파티션별 메시지 처리
def process_partition(partition_id):
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
            if data['code'] in coin_codes:
                buffer.append(data)

            # 고정 배치 크기 처리
            if len(buffer) >= BATCH_SIZE:
                with LOCKS[partition_id]:  # 파티션별 Lock
                    process_records_parallel(buffer)
                    consumer.commit()
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

    for partition_id in PARTITION_LIST:
        thread = threading.Thread(target=process_partition, args=(partition_id,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    print("[INFO] Starting Kafka Consumer with optimized multi-threading...")
    main()
