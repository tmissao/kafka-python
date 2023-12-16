from config.AppConfig import AppConfig
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.consumer.fetcher import ConsumerRecord
import json

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'test.topic.3'
KAFKA_GROUP_ID = 'python-consumer-test'


def get_kafka_consumer(bootstrap_servers: list[str], topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        key_deserializer=lambda data: json.loads(data.decode('UTF-8')),
        value_deserializer=lambda data: json.loads(data.decode('UTF-8')),
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


def run() -> None:
    config = AppConfig.instance()
    consume_messages(get_kafka_consumer(config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_SIMPLE_TOPIC,
                                        config.KAFKA_SIMPLE_CONSUMER_GROUP_ID))


if __name__ == '__main__':
    run()
