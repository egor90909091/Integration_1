import json

from common import connect, setup, ORDERS_QUEUE, PARTS_QUEUE, WORK_NAMES

connection = connect()
channel = connection.channel()
setup(channel)


def on_order(ch, method, properties, body):
    order = json.loads(body)
    works = order["works"]
    print(f"заявка {properties.correlation_id} ({order['well']}): делим на {len(works)} части")

    for work in works:
        part = {
            "well": order["well"],
            "type": work["type"],
            "description": work["description"],
            "total": len(works),
        }
        ch.basic_publish(
            exchange="",
            routing_key=PARTS_QUEUE,
            body=json.dumps(part, ensure_ascii=False),
            properties=properties,
        )
        print(f"  часть: {WORK_NAMES[work['type']]} - {work['description']}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=ORDERS_QUEUE, on_message_callback=on_order)

print("разделитель запущен")
channel.start_consuming()
