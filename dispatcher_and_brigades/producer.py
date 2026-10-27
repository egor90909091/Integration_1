import json
import random
import time
import pika

from common import connect, setup, QUEUE

wells = [
    "Скважина 1052",
    "Скважина 1112",
    "Скважина 2026",
]

works = [
    "замена датчика давления",
    "ликвидация обрыва насосных штанг",
    "замена кабельной линии",
    "замена ремней на станке-качалке",
    "устранение пропуска нефти на фланце",
    "утечка газа",
]

operators = [ "Савин", "Сидоров", "Кузнецов"]

connection = connect()
channel = connection.channel()
setup(channel)

print("Диспетчерская запущена")

request_id = 1
while True:
    request = {
        "id": request_id,
        "well": random.choice(wells),
        "work": random.choice(works),
        "operator": random.choice(operators),
    }

    channel.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps(request, ensure_ascii=False),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"Отправлена заявка #{request['id']}: {request['well']} - {request['work']} (оператор {request['operator']})")

    request_id += 1
    time.sleep(3)
