import json
import os
import random
import time

from common import connect, setup, QUEUE

BRIGADE = os.getenv("BRIGADE", "Бригада")

connection = connect()
channel = connection.channel()
setup(channel)

channel.basic_qos(prefetch_count=1)


def on_message(ch, method, properties, body):
    request = json.loads(body)
    print(f"[{BRIGADE}] Получена заявка #{request['id']}: {request['well']} - {request['work']}")

    time.sleep(random.randint(3, 7))

    print(f"[{BRIGADE}] Заявка #{request['id']} выполнена ")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=QUEUE, on_message_callback=on_message)

print(f"[{BRIGADE}] Ждет заявку")
channel.start_consuming()
