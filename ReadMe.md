# Kafka Python Application
---

This application intends to show how to build Kafka Consumers and Producers using Python. There were constructed three types of consumers/producers

## Producers

- [`Simple Producer`](./app/producer/simple-producer.py) - Just generates a Json record an send it to Kafka
- [`Avro Producer`](./app/producer/avro-producer.py) - Generates a record serializing it using Avro Protocol and saving its schema on schema registry.
- [`Avro Confluent Producer`](./app/producer/avro-confluent-producer.py) - Generates a record serializing it using Avro Protocol accordingly with Confluent Pattern (including confluent magical bytes) and saving its schema on schema registry.


## Consumers

- [`Simple Consumer`](./app/consumer/simple-consumer.py) - consumes a Json record from Kafka
- [`Avro Consumer`](./app/consumer/avro-consumer.py) - consumes an Avro serialized record from Kafka, obtaining the avro schema from schema registry.
- [`Avro Confluent Consumer`](./app/consumer/avro-confluent-consumer.py) - consumes an Avro serialized record from Kafka, dealing with confluent magical bytes and obtaining the avro schema from schema registry.

## Setup
---

- [Install Docker](https://docs.docker.com/engine/install/ubuntu/)
- [Install Docker-Compose](https://docs.docker.com/compose/install/)
- [Install PyEnv](https://github.com/pyenv/pyenv) - optional [link](https://realpython.com/intro-to-pyenv/)


## Running Kafka Cluster
---

In order to run the Kafka Cluster execute the following commands:
```bash
docker-compose up -d
```

After that a kafka broker will be available at `localhost:9092`. Also, a web UI will be available at `localhost:3030`


## Running Python Application
---

In order to run the python applications run the following commands:

```bash
# Application directory
cd app

# Installs the correct python version (3.9)
pyenv local

# Creates Python Virtual Environment
python -m venv .venv
source .venv/bin/activate

# Installs required libraries
pip install -r requirements.txt

# Add the current directory to be included in the search path for python binaries
export PYTHONPATH=.

# Execute the producers
python producer/simple-producer.py
python producer/avro-producer.py
python producer/avro-confluent-producer.py

# Execute the consumers
python consumer/simple-consumer.py
python consumer/avro-consumer.py
python python consumer/avro-confluent-consumer.py

```



## References
---

- [`Kafka Python Library`](https://kafka-python.readthedocs.io/en/master/)
- [`Kafka Python Tutorial`](https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1)
- [`Kafka Python Tutorial 2`](https://hevodata.com/learn/kafka-python/)
- [`Avro Python`](https://hevodata.com/learn/kafka-python/)
- [`Decoding Avro Confluent with Python`](https://stackoverflow.com/questions/44407780/how-to-decode-deserialize-avro-with-python-from-kafka)
- [`Kafka Serialize Avro Record with Python`](https://stackoverflow.com/questions/70008025/kafka-avro-python)
- [`Schema Registry API Reference`](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)