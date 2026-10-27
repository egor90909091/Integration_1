import json
import os

from common import connect, setup, EXCHANGE, DEAD_EXCHANGE, MESSAGE_TTL

DEPARTMENT = os.getenv("DEPARTMENT", "Отдел")
QUEUE = os.getenv("QUEUE", "department")

connection = connect()
channel = connection.channel()
setup(channel)

channel.queue_declare(
    queue=QUEUE,
    durable=True,
    arguments={
        "x-message-ttl": MESSAGE_TTL,
        "x-dead-letter-exchange": DEAD_EXCHANGE,
    },
)
channel.queue_bind(queue=QUEUE, exchange=EXCHANGE)


def on_message(ch, method, properties, body):
    news = json.loads(body)
    print(f"[{DEPARTMENT}] Получена новость #{news['id']}: цена барреля {news['price']}$ ({news['change']:+}), опубликовано в {news['time']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=QUEUE, on_message_callback=on_message)

print(f"[{DEPARTMENT}] Подписан на получения цены")
channel.start_consuming()
