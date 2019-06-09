#!/usr/bin/env python3

import calendar
import json
import logging
import os
import socket
import time
import traceback
from datetime import datetime
from random import uniform
from time import sleep
from typing import Tuple
from uuid import uuid4

import pika


def _setup_logger() -> None:
    logging.basicConfig(format='%(levelname)s | %(asctime)s UTC | %(message)s', level=logging.INFO)
    logging.Formatter.converter = time.gmtime


def _random_time(base_s: float) -> float:
    return uniform(0, 1.) * base_s


def dt_to_timestamp(dt: datetime) -> Tuple[int, int]:
    return calendar.timegm(dt.utctimetuple()), dt.microsecond


if __name__ == '__main__':
    _setup_logger()
    host_name = socket.gethostname()
    logging.info(f"Reading params on {host_name}...")
    rmq_host, rmq_exchange, rmq_queue = os.environ["RMQ_HOST"], os.environ["RMQ_EXCHANGE"], os.environ["RMQ_QUEUE"]
    rmq_user, rmq_pass = os.environ["RMQ_USER"], os.environ["RMQ_PASS"]
    produce_s = 0.001 * int(os.getenv("PRODUCE_MS"))
    consume_s = 0.001 * int(os.getenv("CONSUME_MS"))
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rmq_host,
                                                                   credentials=pika.credentials.PlainCredentials(
                                                                       username=rmq_user, password=rmq_pass)))
    channel = connection.channel()
    channel.queue_declare(queue=rmq_queue)
    logging.info(
        f"Sending messages to {rmq_host} to exchange {rmq_exchange} and queue {rmq_queue} with interval of {produce_s}s...")
    msg_count = 0
    while True:
        if msg_count % 10 == 0:
            logging.info(f"Already sent {msg_count} messages...")
        try:
            epoch, microseconds = dt_to_timestamp(datetime.utcnow())
            msg = {"id": str(uuid4()), "sender": host_name,
                   "sent_epoch": epoch, "sent_microseconds": microseconds, "task_time_s": _random_time(consume_s)}
            channel.basic_publish(exchange=rmq_exchange, routing_key=rmq_queue, body=json.dumps(msg))
            msg_count += 1
            logging.debug(f"Sent message #{msg_count}")
            sleep(_random_time(produce_s))
        except KeyboardInterrupt:
            logging.warning("Keyboard interrupt, exiting!")
            break
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
            sleep(2.)
    if connection:
        connection.close()
    logging.info(f"Sent {msg_count} messages to {rmq_host} to exchange {rmq_exchange} and queue {rmq_queue}")
