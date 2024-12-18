# Запис даних у топік
from kafka import KafkaProducer
from create_config import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Назва топіку
my_name = "lina"
building_sensors = f"{my_name}_spark_streaming_in"


# Відправлення 30 повідомлення в топік
sensor_id = random.randint(1, 10000)
for i in range(30):
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "temperature": random.randint(25, 45),  # Випадкове значення температури
            "humidity": random.randint(15, 85),  # Випадкове значення вологості
            "number_of_sensors": sensor_id,  # Випадкове значення кількості датчиків
        }
        producer.send(building_sensors, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{building_sensors}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer
