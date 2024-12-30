from kafka import KafkaConsumer
import boto3
import json
import decimal
from concurrent.futures import ThreadPoolExecutor

# DynamoDB 설정
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
table = dynamodb.Table('dynamoDB_upbit_table')

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'upbit-ticker-data',
    bootstrap_servers=['spark-worker-panda-01:9092',
                       'spark-worker-panda-02:9092',
                       'spark-worker-panda-03:9092',
                       'spark-worker-panda-04:9092'],
    group_id='upbit-consumer-group',
    auto_offset_reset='latest',
    enable_auto_commit=False,  # 수동 커밋
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 배치 및 병렬 처리 설정
BATCH_SIZE = 25
THREADS = 10

# DynamoDB에 배치 쓰기 함수
def write_batch_to_dynamodb(records):
    for record in records:
        try:
            # 중복 방지 조건부 삽입
            table.put_item(
                Item={
                    "code": record['code'],
                    "trade_timestamp": int(record['trade_timestamp']),
                    "timestamp": int(record['timestamp']),
                    "high_price": decimal.Decimal(str(record['high_price'])),
                    "low_price": decimal.Decimal(str(record['low_price'])),
                    "trade_price": decimal.Decimal(str(record['trade_price'])),
                    "change": record['change'],
                    "change_price": decimal.Decimal(str(record['change_price'])),
                    "change_rate": decimal.Decimal(str(record['change_rate']))
                },
                ConditionExpression="attribute_not_exists(code) AND attribute_not_exists(trade_timestamp)"  # 중복 방지
            )
        except Exception as e:
            # 중복 키 에러 무시
            if "ConditionalCheckFailedException" not in str(e):
                print(f"Failed to write record to DynamoDB: {e}")

# 멀티스레드 병렬 처리
def process_records_parallel(records):
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            executor.submit(write_batch_to_dynamodb, batch)

# Kafka 데이터 처리 루프
print("Listening for messages...")
buffer = []

try:
    for message in consumer:
        try:
            # 메시지 데이터 추출
            data = message.value
            buffer.append(data)

            # 배치 크기 도달 시 병렬 처리
            if len(buffer) >= BATCH_SIZE:
                process_records_parallel(buffer)  # 병렬 처리
                consumer.commit()                # 커밋 (배치 처리 후)
                buffer.clear()                   # 버퍼 초기화

        except Exception as e:
            print(f"Error processing message: {e}")
            continue  # 예외 발생 시 다음 메시지 처리

except KeyboardInterrupt:
    print("Stopping Kafka Consumer...")

finally:
    if buffer:  # 남은 버퍼 처리
        process_records_parallel(buffer)
        consumer.commit()
    consumer.close()
