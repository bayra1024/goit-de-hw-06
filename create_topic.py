# Створення топіку


# Створіть файл create_topic.py з таким вмістом:
# Імпорт модулів
# KafkaAdminClient — клас для взаємодії з адміністративними функціями Kafka;
# NewTopic — клас для визначення нового топіку;
# kafka_config — конфігураційний файл, що містить налаштування Kafka.


from kafka.admin import KafkaAdminClient, NewTopic
from create_config import kafka_config

# Створення клієнта Kafka

# bootstrap_servers — список серверів Kafka;
# security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password —
# параметри для налаштування безпеки та автентифікації.

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

# Визначення нового топіку

# my_name — ім'я користувача, яке використовується для створення імені топіку;
# topic_name — назва нового топіку;
# num_partitions — кількість партицій у топіку;
# replication_factor — коефіцієнт реплікації для забезпечення стійкості даних (ми будемо використовувати тільки значення 1);
# new_topic — об'єкт нового топіку, який визначає його параметри.

my_name = "lina"
building_sensors = f"{my_name}_spark_streaming_in"
alerts = f"{my_name}_alerts"
num_partitions = 2
replication_factor = 1

new_topic_1 = NewTopic(
    name=building_sensors,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

new_topic_2 = NewTopic(
    name=alerts,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

new_topic = [new_topic_1, new_topic_2]

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=new_topic, validate_only=False)
    # print(f"Topic '{topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Виведення існуючих топіків на екран та закриття клієнта адміністратора
[print(topic) for topic in admin_client.list_topics() if "lina" in topic]

# Закриття зв'язку з клієнтом
admin_client.close()
