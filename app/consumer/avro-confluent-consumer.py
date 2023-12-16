import io
import requests
import avro.schema
from avro.io import DatumReader, BinaryDecoder
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.consumer.fetcher import ConsumerRecord
from config.AppConfig import AppConfig


def get_avro_schema(schema_registry_url, subject_name) -> str:
    endpoint = f'{schema_registry_url}/subjects/{subject_name}/versions/latest/schema'
    return requests.get(url=endpoint).text


def remove_confluent_magical_bytes(data: bytes) -> io.BytesIO:
    message_bytes = io.BytesIO(data)
    # Ignores Confluent Avro Magical Bytes
    message_bytes.seek(5)
    return message_bytes


def get_kafka_consumer(bootstrap_servers: list[str], topic: str, group_id: str,
                       schema_registry_url: str) -> KafkaConsumer:

    avro_key_schema = avro.schema.parse(get_avro_schema(schema_registry_url, f'{topic}-key'))
    avro_value_schema = avro.schema.parse(get_avro_schema(schema_registry_url, f'{topic}-value'))

    def avro_deserializer(avro_schema, data) -> bytes:
        message_bytes = remove_confluent_magical_bytes(data)
        reader = DatumReader(avro_schema)
        decoder = BinaryDecoder(message_bytes)
        return reader.read(decoder)

    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        key_deserializer=lambda data: avro_deserializer(avro_key_schema, data),
        value_deserializer=lambda data: avro_deserializer(avro_value_schema, data),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=group_id
    )


def create_message_offset(message: ConsumerRecord) -> dict[TopicPartition, OffsetAndMetadata]:
    topic_partition = TopicPartition(message.topic, message.partition)
    offset_metadata = OffsetAndMetadata(message.offset + 1, message.timestamp)
    return {topic_partition: offset_metadata}


def consume_messages(consumer: KafkaConsumer) -> None:
    processed_offsets = {}
    try:
        for message in consumer:
            # process message
            print(f'Key={message.key}\nValue={message.value}\nPartition={message.partition}\nOffset={message.offset}')
            offset = create_message_offset(message)
            processed_offsets.update(offset)
            consumer.commit_async(offset)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.commit(processed_offsets)
        consumer.close(autocommit=False)


def run():
    config = AppConfig.instance()
    consume_messages(get_kafka_consumer(config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_AVRO_CONFLUENT_TOPIC,
                                        config.KAFKA_AVRO_CONFLUENT_CONSUMER_GROUP_ID, config.SCHEMA_REGISTRY_URL))


if __name__ == '__main__':
    run()
