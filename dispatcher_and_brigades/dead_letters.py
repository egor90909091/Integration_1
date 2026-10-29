import json

from common import connect, setup, DEAD_QUEUE

connection = connect()
channel = connection.channel()
setup(channel)


def on_message(ch, method, properties, body):
    request = json.loads(body)
    reason = properties.headers["x-death"][0]["reason"]
    print(f"НЕ ДОСТАВЛЕНО: заявка #{request['id']} {request['well']} - {request['work']} (причина: {reason})")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=DEAD_QUEUE, on_message_callback=on_message)

print("Слушаю канал недоставленных заявок")
channel.start_consuming()
