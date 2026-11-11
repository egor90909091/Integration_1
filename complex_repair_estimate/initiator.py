import json
import random
import uuid

import pika

from common import connect, setup, ORDERS_QUEUE, WORK_NAMES

REPLY_QUEUE = "initiator_replies"
wells = ["Скважина 1052", "Скважина 1112", "Скважина 2026", "Скважина 3108"]

works = {
    "drilling": ["бурение новой скважины", "углубление скважины", "бурение бокового ствола"],
    "electric": ["замена кабеля", "ремонт щитка управления", "замена трансформатора", "замена проводки"],
    "logistics": ["доставка топлива", "доставка труб", "перевозка бригады", "перевозка техники"],
}

connection = connect()
channel = connection.channel()
setup(channel)

channel.queue_declare(queue=REPLY_QUEUE, durable=True)


def on_estimate(ch, method, properties, body):
    estimate = json.loads(body)
    print(f"Получена смета по заявке {properties.correlation_id} ({estimate['well']}):")
    for item in estimate["items"]:
        print(f"  {WORK_NAMES[item['type']]}: {item['description']} - {item['cost']} тыс. руб, {item['days']} дн. ({item['service']})")
    print(f"  итого: {estimate['total_cost']} тыс. руб, {estimate['total_days']} дн.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=REPLY_QUEUE, on_message_callback=on_estimate)
print("инициатор запущен")

while True:
    correlation_id = uuid.uuid4().hex[:8]

    types = random.sample(list(works), k=random.randint(2, 3))
    order = {
        "well": random.choice(wells),
        "works": [{"type": t, "description": random.choice(works[t])} for t in types],
    }

    channel.basic_publish(
        exchange="",
        routing_key=ORDERS_QUEUE,
        body=json.dumps(order, ensure_ascii=False),
        properties=pika.BasicProperties(
            correlation_id=correlation_id,
            reply_to=REPLY_QUEUE,
            delivery_mode=2,
        ),
    )
    names = ", ".join(WORK_NAMES[t] for t in types)
    print(f"Отправлена заявка {correlation_id}: {order['well']} ({names})")

    connection.process_data_events(time_limit=8)
