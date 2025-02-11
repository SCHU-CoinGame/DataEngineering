import redis

# Redis 연결 설정
redis_host = "coincache-ek5phj.serverless.apn2.cache.amazonaws.com"
redis_port = 6379  # 기본 포트
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

# Redis 데이터 확인 함수
def check_redis_data():
    try:
        # 저장된 키들 가져오기
        keys = redis_client.keys("coin:*")
        print(f"[INFO] Found {len(keys)} keys in Redis.")

        # 각 키의 데이터 확인
        for key in keys:
            value = redis_client.hgetall(key)
            print(f"[KEY: {key}] -> {value}")

    except Exception as e:
        print(f"[ERROR] Failed to fetch data from Redis: {e}")

if __name__ == "__main__":
    check_redis_data()
