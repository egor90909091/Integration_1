import json
import os
import random
import time

from common import connect, setup, RESULTS_QUEUE, WORK_QUEUES

WORK_TYPE = os.getenv("WORK_TYPE", "drilling")
SERVICE = os.getenv("SERVICE", "Сервис")

rates = {
    "drilling": (760, 2520, 5, 15),
    "electric": (150, 650, 1, 4),
    "logistics": (48, 252, 1, 3),
}
min_cost, max_cost, min_days, max_days = rates[WORK_TYPE]

connection = connect()
channel = connection.channel()
setup(channel)

channel.basic_qos(prefetch_count=1)

def on_task(ch, method, properties, body):
    task = json.loads(body)
    print(f"[{SERVICE}] Заявка {properties.correlation_id}: считаем '{task['description']}' ({task['well']})")

    time.sleep(random.randint(1, 5))

    task["service"] = SERVICE
    task["cost"] = random.randint(min_cost, max_cost)
    task["days"] = random.randint(min_days, max_days)

    ch.basic_publish(
        exchange="",
        routing_key=RESULTS_QUEUE,
        body=json.dumps(task, ensure_ascii=False),
        properties=properties,
    )
    print(f"[{SERVICE}] Заявка {properties.correlation_id}: {task['cost']} тыс. руб, {task['days']} дн.")

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=WORK_QUEUES[WORK_TYPE], on_message_callback=on_task)

print(f"[{SERVICE}] ожидает заявки на расчет")
channel.start_consuming()
