import json

from common import connect, setup, DEAD_QUEUE

connection = connect()
channel = connection.channel()
setup(channel)


def on_message(ch, method, properties, body):
    news = json.loads(body)
    death = properties.headers["x-death"][0]

    print(f"НЕ ДОСТАВЛЕНО в очередь {death['queue']}: новость #{news['id']}, цена {news['price']}$ (причина: {death['reason']})")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=DEAD_QUEUE, on_message_callback=on_message)

print("Слушаю канал недоставленных новостей ")
channel.start_consuming()
