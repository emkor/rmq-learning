#!/usr/bin/env python3

import logging
import os
import socket
import traceback
from datetime import datetime

import pika

from common import setup_logger, Task, do


def _msg_handle(ch, method, properties, body):
    task = Task.from_json(body)
    if task.param >= 0.9 and datetime.utcnow().microsecond % 2 == 0:
        logging.warning(f"Failing task: {task.uuid}, re-queueing...")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    else:
        done_task = do(task)
        logging.info(f"{done_task.worker} done {done_task.uuid} sent by {done_task.sender} in {done_task.took():.3f}s!")
        ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    setup_logger()
    host_name = socket.gethostname()
    msg_count = 0
    logging.info(f"Reading params on {host_name}...")
    rmq_host, rmq_exchange, rmq_queue = os.environ["RMQ_HOST"], os.environ["RMQ_EXCHANGE"], os.environ["RMQ_QUEUE"]
    rmq_user, rmq_pass = os.environ["RMQ_USER"], os.environ["RMQ_PASS"]
    rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=rmq_host,
                                                                 credentials=pika.credentials.PlainCredentials(
                                                                     username=rmq_user, password=rmq_pass)))
    rmq_chan = rmq_conn.channel()
    rmq_chan.queue_declare(queue=rmq_queue, durable=True)
    rmq_chan.basic_consume(queue=rmq_queue, on_message_callback=_msg_handle)
    logging.info(
        f"Consuming messages from {rmq_host} exchange {rmq_exchange} and queue {rmq_queue}...")
    try:
        rmq_chan.start_consuming()
    except KeyboardInterrupt:
        logging.warning("Keyboard interrupt, exiting!")
    except Exception as e:
        traceback.print_exc()
        logging.error(e)
    finally:
        if rmq_conn:
            rmq_conn.close()
    logging.info(f"Consumed {msg_count} messages to {rmq_host} to exchange {rmq_exchange} and queue {rmq_queue}")
