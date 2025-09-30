from kafka import KafkaConsumer
import json

def consume_messages(topic, expected_count=10):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    messages = []
    for message in consumer:
        messages.append(message.value)
        if len(messages) >= expected_count:
            break
    return messages
