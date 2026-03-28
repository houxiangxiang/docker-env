from kafka import KafkaProducer
import json
import time

# 1. 创建 Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka 地址
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'test-topic'

# 2. 发送消息
for i in range(5):
    message = {
        "id": i,
        "msg": f"hello kafka {i}"
    }

    producer.send(topic_name, value=message)
    print(f"send: {message}")

    time.sleep(1)

# 3. 确保消息发送完成
producer.flush()
producer.close()