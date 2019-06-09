#!/usr/bin/env python3

import json
import logging
import os
import socket
import time
import traceback
from datetime import datetime, timedelta
from random import uniform
from time import sleep

import pika


def _setup_logger() -> None:
    logging.basicConfig(format='%(levelname)s | %(asctime)s UTC | %(message)s', level=logging.INFO)
    logging.Formatter.converter = time.gmtime


def _random_time(base_s: float) -> float:
    return uniform(0, 1.) * base_s


def timestamp_to_dt(timestamp: int, microseconds: int) -> datetime:
    return datetime.utcfromtimestamp(timestamp) + timedelta(microseconds)


def _msg_handle(ch, method, properties, body):
    curr_time = datetime.utcnow()
    msg = json.loads(body)
    msg_id, sender, task_time_s = msg["id"], msg["sender"], msg["task_time_s"]
    sent_time = timestamp_to_dt(msg["sent_epoch"], msg["sent_microseconds"])
    took_s = (curr_time - sent_time).total_seconds()
    logging.info(f"{host_name} got message {msg_id} from queue {ch} sent by {sender} in {took_s}s!")
    sleep(_random_time(task_time_s))


if __name__ == '__main__':
    _setup_logger()
    host_name = socket.gethostname()
    msg_count = 0
    logging.info(f"Reading params on {host_name}...")
    rmq_host, rmq_exchange, rmq_queue = os.environ["RMQ_HOST"], os.environ["RMQ_EXCHANGE"], os.environ["RMQ_QUEUE"]
    rmq_user, rmq_pass = os.environ["RMQ_USER"], os.environ["RMQ_PASS"]
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rmq_host,
                                                                   credentials=pika.credentials.PlainCredentials(
                                                                       username=rmq_user, password=rmq_pass)))
    channel = connection.channel()
    channel.queue_declare(queue=rmq_queue)
    channel.basic_consume(queue=rmq_queue, auto_ack=True, on_message_callback=_msg_handle)
    logging.info(
        f"Consuming messages from {rmq_host} exchange {rmq_exchange} and queue {rmq_queue}...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.warning("Keyboard interrupt, exiting!")
    except Exception as e:
        traceback.print_exc()
        logging.error(e)
    finally:
        if connection:
            connection.close()
        logging.info(f"Consumed {msg_count} messages to {rmq_host} to exchange {rmq_exchange} and queue {rmq_queue}")
