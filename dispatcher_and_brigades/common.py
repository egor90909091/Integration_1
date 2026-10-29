import os

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")

QUEUE = "repair_requests"
DEAD_EXCHANGE = "repair_dead"
DEAD_QUEUE = "repair_dead_letters"

MESSAGE_TTL = 10000


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))


def setup(channel):
    channel.exchange_declare(exchange=DEAD_EXCHANGE, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)
    channel.queue_bind(queue=DEAD_QUEUE, exchange=DEAD_EXCHANGE)


    channel.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": MESSAGE_TTL,
            "x-dead-letter-exchange": DEAD_EXCHANGE,
        },
    )
