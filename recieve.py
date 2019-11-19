#!/usr/bin/env python
import time

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)

def callback(ch, method, properties, body):
    try:
        print(f" [x] Received {body}")
        time.sleep(body.count(b'.'))
        print(f" [x] Done")
        ch.basic_ack(delivery_tag = method.delivery_tag)
    except:
        print('error')

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
