import json
import pika
from src.common.config import settings

QUEUE_NAME = "jobs"

class Rabbit:
    def __init__(self):
        params = pika.URLParameters(settings.rabbitmq_url)
        params.heartbeat = 30
        params.blocked_connection_timeout = 300

        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
        self.ch.queue_declare(queue=QUEUE_NAME, durable=True)

    def publish(self, body: dict):
        self.ch.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(body).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def consume(self, on_message):
        self.ch.basic_qos(prefetch_count=settings.max_concurrency)
        self.ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
        self.ch.start_consuming()

    def ack_threadsafe(self, delivery_tag):
        self.conn.add_callback_threadsafe(lambda: self.ch.basic_ack(delivery_tag))

    def nack_threadsafe(self, delivery_tag, requeue=True):
        self.conn.add_callback_threadsafe(lambda: self.ch.basic_nack(delivery_tag, requeue=requeue))