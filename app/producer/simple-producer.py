from kafka import KafkaProducer
from kafka.errors import KafkaError
from config.AppConfig import AppConfig
import json
import random
import traceback

NUMBER_OF_MESSAGES_TO_SEND: int = 50


def get_kafka_producer(bootstrap_servers: list[str]) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda data: json.dumps(data).encode('UTF-8'),
        value_serializer=lambda data: json.dumps(data).encode('UTF-8'),
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
    produce_message(get_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS), config.KAFKA_SIMPLE_TOPIC,
                    NUMBER_OF_MESSAGES_TO_SEND)


if __name__ == '__main__':
    run()
