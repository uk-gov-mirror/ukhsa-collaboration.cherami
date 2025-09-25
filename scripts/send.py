#!/usr/bin/env python3
import json

import pika

RABBITMQ_IP = "192.168.0.15"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_IP, credentials=pika.PlainCredentials("admin", "password"))
)
channel = connection.channel()

# # define exchange
# channel.exchange_declare(exchange="test", exchange_type="fanout", durable=True)

body = json.dumps({"sample_id": "test123"}, ensure_ascii=False)
properties = pika.BasicProperties(
    content_type="json",
    delivery_mode=pika.DeliveryMode.Persistent,
)

channel.basic_publish(exchange="cherami_test", routing_key="", body=body, properties=properties)
print("sent")
connection.close()
