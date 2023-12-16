from __future__ import annotations
import os
from typing import get_type_hints, Union


def _parse_bool(val: Union[str, bool]) -> bool:
    return val if isinstance(type(val), bool) else val.lower() in ['true', 'yes', '1']


class AppConfigError(Exception):
    pass


# AppConfig class with required fields, default values, type checking, and typecasting for int and bool values
class AppConfig:
    KAFKA_BOOTSTRAP_SERVERS: list[str] = ['localhost:9092']
    SCHEMA_REGISTRY_URL: str = 'http://localhost:8081'
    KAFKA_SIMPLE_TOPIC: str = "test.simple.topic"
    KAFKA_SIMPLE_CONSUMER_GROUP_ID: str = "python-simple-consumer-group-id"
    KAFKA_AVRO_TOPIC: str = "test.avro.topic"
    KAFKA_AVRO_CONSUMER_GROUP_ID: str = "python-avro-consumer-group-id"
    KAFKA_AVRO_CONFLUENT_TOPIC: str = "test.avro.confluent.topic"
    KAFKA_AVRO_CONFLUENT_CONSUMER_GROUP_ID: str = "python-avro-confluent-consumer-group-id"

    _instance: AppConfig = None

    def __init__(self, env):
        for field in self.__annotations__:
            if not field.isupper():
                continue

            # Raise AppConfigError if required field not supplied
            default_value = getattr(self, field, None)
            if default_value is None and env.get(field) is None:
                raise AppConfigError(f'The {field} field is required')

            # Cast env var value to expected type and raise AppConfigError on failure
            try:
                var_type = get_type_hints(AppConfig)[field]
                if var_type == bool:
                    value = _parse_bool(env.get(field, default_value))
                else:
                    value = var_type(env.get(field, default_value))

                self.__setattr__(field, value)
            except ValueError:
                raise AppConfigError(f'Unable to cast value of "{env[field]}"'
                                     f'to type "{var_type}" for "{field}" field') from ValueError

    def __repr__(self) -> str:
        return str(self.__dict__)

    @classmethod
    def instance(cls) -> AppConfig:
        if cls._instance is None:
            cls._instance = cls(os.environ)
        return cls._instance
