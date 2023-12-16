import io

import avro.schema
import requests
from avro.io import DatumWriter, BinaryEncoder
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config.AppConfig import AppConfig
import random
import traceback

NUMBER_OF_MESSAGES_TO_SEND: int = 15


def save_avro_schema(schema_registry_url, subject_name, avro_schema):
    endpoint = f'{schema_registry_url}/subjects/{subject_name}/versions'
    result = requests.post(url=endpoint, json={'schema': avro_schema})
    return result.json()['id']


def get_avro_schema(avro_schema_path: str) -> str:
    return open(avro_schema_path).read()


# Confluent appends 5 special bytes on every avro record, in which:
# First byte has the fixed value of zero
# Second to Fifth bytes is the schema registry id
def create_confluent_magical_bytes(schema_registry_id) -> bytes:
    magical_byte = (0).to_bytes(1, 'big')
    schema_registry_id_bytes = schema_registry_id.to_bytes(4, 'big')
    return b''.join([magical_byte, schema_registry_id_bytes])


def get_kafka_producer(bootstrap_servers: list[str], topic_name: str, schema_registry_url: str) -> KafkaProducer:
    avro_key_schema_raw = get_avro_schema('./avro/message-key.avsc')
    avro_key_schema_id = save_avro_schema(schema_registry_url, f'{topic_name}-key', avro_key_schema_raw)
    avro_key_schema = avro.schema.parse(avro_key_schema_raw)
    avro_key_magical_bytes = create_confluent_magical_bytes(avro_key_schema_id)

    avro_value_schema_raw = get_avro_schema('./avro/message-value.avsc')
    avro_value_schema_id = save_avro_schema(schema_registry_url, f'{topic_name}-value', avro_value_schema_raw)
    avro_value_schema = avro.schema.parse(avro_value_schema_raw)
    avro_value_magical_bytes = create_confluent_magical_bytes(avro_value_schema_id)

    def avro_serializer(avro_schema, magical_bytes, data) -> bytes:
        bytes_writer = io.BytesIO()
        bytes_writer.write(magical_bytes)
        writer = DatumWriter(writer_schema=avro_schema)
        encoder = BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return bytes_writer.getvalue()

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda data: avro_serializer(avro_key_schema, avro_key_magical_bytes, data),
        value_serializer=lambda data: avro_serializer(avro_value_schema, avro_value_magical_bytes, data),
    )


def produce_message(producer: KafkaProducer, kafka_topic: str, number_of_messages: int) -> None:
    try:
        for n in range(number_of_messages):
            data = {'id': n, 'luck_number':  random.randint(0, 10000)}
            producer.send(topic=kafka_topic, value=data, key={'id': n})
        producer.flush()
    except KafkaError as error:
        print(error)
        print('this error should be treated otherwise message is not synced')
        traceback.print_exc()


def run():
    config = AppConfig.instance()
    produce_message(get_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_AVRO_CONFLUENT_TOPIC,
                                       config.SCHEMA_REGISTRY_URL),
                    config.KAFKA_AVRO_CONFLUENT_TOPIC, NUMBER_OF_MESSAGES_TO_SEND)


if __name__ == '__main__':
    run()
