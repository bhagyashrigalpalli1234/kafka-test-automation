from producer import send_message
from consumer import consume_messages

TOPIC = 'demo-automation'

def test_kafka_end_to_end():
    test_data = [{"message": f"msg_{i}"} for i in range(10)]

    # Send messages
    for msg in test_data:
        send_message(TOPIC, msg)

    # Consume and assert
    received = consume_messages(TOPIC, expected_count=10)

    assert len(received) == 10
    assert received == test_data


