from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from create_config import kafka_config
import os


my_name = "lina"
building_sensors = f"{my_name}_spark_streaming_in"

# Пакет, необхідний для читання Kafka зі Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Створення SparkSession
spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", building_sensors)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .load()
)

# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON.
json_schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("number_of_sensors", IntegerType(), True),
    ]
)

# Маніпуляції з даними
clean_df = (
    df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn(
        "timestamp",
        from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp"),
    )
    .withColumn("temperature", col("value_json.temperature"))
    .withColumn("humidity", col("value_json.humidity"))
    .withColumn("number_of_sensors", col("value_json.number_of_sensors"))
    .drop("value_json", "value_deserialized")
)

# Виведення даних на екран
displaying_df = (
    clean_df.writeStream.trigger(availableNow=True)
    .outputMode("append")
    .format("console")
    .option("checkpointLocation", "/tmp/checkpoints-2")
    .start()
    .awaitTermination()
)

# # Підготовка даних для запису в Kafka: формування ключ-значення
# prepare_to_kafka_df = clean_df.select(
#     col("key"), to_json(struct(col("value"), col("new_value"))).alias("value")
# )

# # Запис оброблених даних у Kafka-топік 'lina_spark_streaming_out'
# query = (
#     prepare_to_kafka_df.writeStream.trigger(processingTime="5 seconds")
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "77.81.230.104:9092")
#     .option("topic", "lina_spark_streaming_out")
#     .option("kafka.security.protocol", "SASL_PLAINTEXT")
#     .option("kafka.sasl.mechanism", "PLAIN")
#     .option(
#         "kafka.sasl.jaas.config",
#         "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
#     )
#     .option("checkpointLocation", "/tmp/checkpoints-3")
#     .start()
#     .awaitTermination()
# )