from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',     # 从最早的消息开始
    enable_auto_commit=True,
    group_id='my-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Waiting for messages...")

for msg in consumer:
    print(f"""
Received message:
  topic: {msg.topic}
  partition: {msg.partition}
  offset: {msg.offset}
  value: {msg.value}
""")