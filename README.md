# rmq-learning
project for learning rabbitmq exchange setup on local cloud

## usage
- start-up RabbitMQ host using `docker-compose -f compose-rmq.yml -d`
- edit `creds.sh` with parameters you want
- on each producer and consumer node with Python 3 available:
    - execute `setup_prod_cons.sh` to download dependencies
    - execute `source creds.sh`
- on producer node:
    - `python3 task_producer.py`
- on consumer node:
    - `python3 task_consumer.py`