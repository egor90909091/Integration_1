import json
import random
import time

import pika

from common import connect, setup, EXCHANGE

connection = connect()
channel = connection.channel()
setup(channel)

print("Отдел трейдинга запущен")

price = 75.0
news_id = 1
while True:
    change = round(random.uniform(-2, 2), 2)
    price = round(price + change, 2)

    news = {
        "id": news_id,
        "price": price,
        "change": change,
        "time": time.strftime("%H:%M:%S"),
    }

    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key="",
        body=json.dumps(news),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"Опубликована новость #{news_id}: цена барреля {price}$ ({change:+})")

    news_id += 1
    time.sleep(5)
