import json

import pika

from common import connect, setup, RESULTS_QUEUE, WORK_NAMES

connection = connect()
channel = connection.channel()
setup(channel)

collected = {}

def on_result(ch, method, properties, body):
    result = json.loads(body)
    correlation_id = properties.correlation_id

    if correlation_id not in collected:
        collected[correlation_id] = []
    collected[correlation_id].append(result)
    got = len(collected[correlation_id])
    print(f"Заявка {correlation_id}: пришел расчет '{WORK_NAMES[result['type']]}' ({got} из {result['total']})")

    if got == result["total"]:
        items = collected.pop(correlation_id)
        estimate = {
            "well": result["well"],
            "items": items,
            "total_cost": sum(item["cost"] for item in items),
            "total_days": sum(item["days"] for item in items),  # работы идут друг за другом
        }

        ch.basic_publish(
            exchange="",
            routing_key=properties.reply_to,
            body=json.dumps(estimate, ensure_ascii=False),
            properties=pika.BasicProperties(correlation_id=correlation_id, delivery_mode=2),
        )
        print(f"Заявка {correlation_id}: смета собрана ({estimate['total_cost']} тысяч рублей, {estimate['total_days']} дней), отправлена в {properties.reply_to}")

    ch.basic_ack(delivery_tag=method.delivery_tag)
channel.basic_consume(queue=RESULTS_QUEUE, on_message_callback=on_result)

print("Агрегатор запущен")
channel.start_consuming()
