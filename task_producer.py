#!/usr/bin/env python3

import logging
import os
import socket
import traceback
from random import uniform
from time import sleep

import pika

from .common import setup_logger, Task

if __name__ == '__main__':
    setup_logger()
    host_name = socket.gethostname()
    logging.info(f"Reading params on {host_name}...")
    rmq_host, rmq_exchange, rmq_queue = os.environ["RMQ_HOST"], os.environ["RMQ_EXCHANGE"], os.environ["RMQ_QUEUE"]
    rmq_user, rmq_pass = os.environ["RMQ_USER"], os.environ["RMQ_PASS"]
    rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=rmq_host,
                                                                 credentials=pika.credentials.PlainCredentials(
                                                                     username=rmq_user, password=rmq_pass)))
    rmq_chan = rmq_conn.channel()
    rmq_chan.basic_qos(prefetch_count=1)
    rmq_chan.queue_declare(queue=rmq_queue, durable=True)
    logging.info(
        f"Sending messages to {rmq_host} to exchange {rmq_exchange} and queue {rmq_queue}...")
    task_cnt = 0
    while True:
        if task_cnt % 10 == 0:
            logging.info(f"Already sent {task_cnt} messages...")
        try:
            task = Task.new(uniform(0., 1.))
            rmq_chan.basic_publish(exchange=rmq_exchange, routing_key=rmq_queue, body=task.to_json())
            task_cnt += 1
            sleep(uniform(0., 1.) / 2.5)
        except KeyboardInterrupt:
            logging.warning("Keyboard interrupt, exiting!")
            break
        except Exception as e:
            traceback.print_exc()
            logging.error(e)
            sleep(1.)
    if rmq_conn:
        rmq_conn.close()
    logging.info(f"Sent {task_cnt} messages to {rmq_host} to exchange {rmq_exchange} and queue {rmq_queue}")
