import os

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")

ORDERS_QUEUE = "repair_orders"

PARTS_QUEUE = "repair_parts"

RESULTS_QUEUE = "estimate_results"

WORK_QUEUES = {
    "drilling": "drilling_tasks",
    "electric": "electric_tasks",
    "logistics": "logistics_tasks",
}

WORK_NAMES = {
    "drilling": "бурение",
    "electric": "электрика",
    "logistics": "логистика",
}


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))

def setup(channel):
    channel.queue_declare(queue=ORDERS_QUEUE, durable=True)
    channel.queue_declare(queue=PARTS_QUEUE, durable=True)
    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)
    for queue in WORK_QUEUES.values():
        channel.queue_declare(queue=queue, durable=True)
