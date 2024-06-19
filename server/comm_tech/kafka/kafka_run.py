from kafka import KafkaConsumer
import json
from datetime import datetime

# Configuration for connecting to your Kafka server
kafka_server = '127.0.0.1:9092'
topic_name = 'OBD2_data'
consumer_timeout_in_ms = 20000

def getCurrentTimestamp():
    now = datetime.now()
    formatted_date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date_time

def kafkaConsumerProcess(queue, no_of_received_msgs_obj, no_of_sent_msgs_obj):
    sent_msg_count = 0
    received_msg_count = 0
    exit_code = 0
    consumer = None

    try:
        consumer = KafkaConsumer(topic_name,
                                 bootstrap_servers=[kafka_server],
                                 auto_offset_reset='earliest',
                                 group_id='my-group',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        print('Listening for messages on topic ' + topic_name + '....')

        for message in consumer:
            current_timestamp = getCurrentTimestamp()

            received_msg = message.value
            if received_msg[0] == "STOP":
                print("Received STOP message")
                sent_msg_count = received_msg[1]
                queue.put("STOP")
                break
            received_msg.append(current_timestamp)
            queue.put(received_msg)
            received_msg_count += 1

    except Exception as e:
        print(f"Kafka consumer error: {e}")
        exit_code = 1
    finally:
        with no_of_sent_msgs_obj.get_lock():
            no_of_sent_msgs_obj.value = sent_msg_count
        with no_of_received_msgs_obj.get_lock():
            no_of_received_msgs_obj.value = received_msg_count

        if consumer is not None:
            consumer.commit()
            consumer.close()

    exit(exit_code)
