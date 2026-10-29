import os

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")

EXCHANGE = "oil_price"

DEAD_EXCHANGE = "oil_price_dead"
DEAD_QUEUE = "oil_price_dead_letters"

MESSAGE_TTL = 10000


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))


def setup(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)

    channel.exchange_declare(exchange=DEAD_EXCHANGE, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)
    channel.queue_bind(queue=DEAD_QUEUE, exchange=DEAD_EXCHANGE)
