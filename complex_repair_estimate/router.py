import json

from common import connect, setup, PARTS_QUEUE, WORK_QUEUES, WORK_NAMES

connection = connect()
channel = connection.channel()
setup(channel)


def on_part(ch, method, properties, body):
    part = json.loads(body)
    queue = WORK_QUEUES[part["type"]]
    ch.basic_publish(exchange="", routing_key=queue, body=body, properties=properties)
    print(f"Заявка {properties.correlation_id}: {WORK_NAMES[part['type']]} -> {queue}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=PARTS_QUEUE, on_message_callback=on_part)

print("маршрутизатор запущен")
channel.start_consuming()
