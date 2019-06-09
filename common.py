import calendar
import json
import logging
import os
import time
from _socket import gethostname
from copy import copy
from datetime import datetime, timedelta
from random import uniform
from typing import Optional, Tuple
from uuid import uuid4


def setup_logger() -> None:
    logging.basicConfig(format="%(asctime)s UTC | %(levelname)s | %(process)d | %(message)s", level=logging.INFO)
    logging.Formatter.converter = time.gmtime


def dt_to_timestamp(dt: datetime) -> Tuple[int, int]:
    return calendar.timegm(dt.utctimetuple()), dt.microsecond


def timestamp_to_dt(timestamp: int, microseconds: int) -> datetime:
    return datetime.utcfromtimestamp(timestamp) + timedelta(microseconds=microseconds)


def curr_proc_host() -> str:
    return str(os.getpid()) + "@" + gethostname()


class Task:
    def __init__(self, uuid: str, sender: str, sent: datetime, param: float) -> None:
        self.uuid = uuid
        self.sender = sender
        self.param = param
        self.sent = sent

    @classmethod
    def new(cls, param: float) -> 'Task':
        return Task(uuid=str(uuid4()), sender=curr_proc_host(), sent=datetime.utcnow(), param=param)

    def to_json(self) -> str:
        as_dict = copy(self.__dict__)
        as_dict["sent_dt"], as_dict["sent_us"] = dt_to_timestamp(as_dict.pop("sent"))
        return json.dumps(as_dict)

    @classmethod
    def from_json(cls, as_json: str) -> 'Task':
        as_dict = json.loads(as_json)
        as_dict["sent"] = timestamp_to_dt(as_dict.pop("sent_dt"), as_dict.pop("sent_us"))
        return Task(**as_dict)

    def __repr__(self):
        return self.to_json()

    def __str__(self):
        return self.to_json()

    def __hash__(self):
        return self.uuid

    def __eq__(self, other):
        return self.uuid == other.uuid


class DoneTask(Task):
    def __init__(self, uuid: str, sender: str, sent: datetime, param: float, got: datetime, done: datetime,
                 worker: str, result: float) -> None:
        super().__init__(uuid, sender, sent, param)
        self.worker = worker
        self.result = result
        self.done = done
        self.got = got

    def to_json(self) -> str:
        as_dict = copy(self.__dict__)
        as_dict["sent_dt"], as_dict["sent_us"] = dt_to_timestamp(as_dict.pop("sent"))
        as_dict["got_dt"], as_dict["got_us"] = dt_to_timestamp(as_dict.pop("got"))
        as_dict["done_dt"], as_dict["done_us"] = dt_to_timestamp(as_dict.pop("done"))
        return json.dumps(as_dict)

    @classmethod
    def from_json(cls, as_json: str) -> 'DoneTask':
        as_dict = json.loads(as_json)
        as_dict["sent"] = timestamp_to_dt(as_dict.pop("sent_dt"), as_dict.pop("sent_us"))
        as_dict["done"] = timestamp_to_dt(as_dict.pop("done_dt"), as_dict.pop("done_us"))
        as_dict["got"] = timestamp_to_dt(as_dict.pop("got_dt"), as_dict.pop("got_us"))
        return DoneTask(**as_dict)

    def took(self) -> float:
        return (self.done - self.got).total_seconds()

    def __repr__(self):
        return self.to_json()

    def __str__(self):
        return self.to_json()

    def __hash__(self):
        return self.uuid

    def __eq__(self, other):
        return self.uuid == other.uuid


def do(task: Task) -> Optional[DoneTask]:
    got = datetime.utcnow()
    result, ops, op_limit = 0, 0, int(task.param * (10 ** 6))
    while ops < op_limit:
        result += 1
        ops += 1
    time.sleep(uniform(0., 0.1))  # simulate I/O
    return DoneTask(uuid=task.uuid, sender=task.sender, sent=task.sent, param=task.param, got=got,
                    worker=curr_proc_host(), done=datetime.utcnow(), result=result)
