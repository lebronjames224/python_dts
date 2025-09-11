import json, pika
from src.common.config import settings

QUEUE_NAME = "jobs"

class Rabbit:
    def __init__(self, heartbeat: int = 0):  # default 0 for idle publishers
        self.params = pika.URLParameters(settings.rabbitmq_url)
        self.params.heartbeat = heartbeat
        self.params.blocked_connection_timeout = 300
        self.params.connection_attempts = 5
        self.params.retry_delay = 2
        self._connect()

    def _connect(self):
        self.conn = pika.BlockingConnection(self.params)
        self.ch = self.conn.channel()
        self.ch.queue_declare(queue=QUEUE_NAME, durable=True)

    def publish(self, body: dict, retry: bool = True):
        payload = json.dumps(body).encode()
        try:
            self.ch.basic_publish(
                exchange="", routing_key=QUEUE_NAME, body=payload,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except Exception:
            if not retry:
                raise
            # Reconnect once and retry
            self._connect()
            self.ch.basic_publish(
                exchange="", routing_key=QUEUE_NAME, body=payload,
                properties=pika.BasicProperties(delivery_mode=2),
            )

    # (workers still need these; scheduler won’t use them)
    def consume(self, on_message):
        self.ch.basic_qos(prefetch_count=settings.max_concurrency)
        self.ch.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
        self.ch.start_consuming()

    def ack_threadsafe(self, delivery_tag):
        self.conn.add_callback_threadsafe(lambda: self.ch.basic_ack(delivery_tag))

    def nack_threadsafe(self, delivery_tag, requeue=True):
        self.conn.add_callback_threadsafe(lambda: self.ch.basic_nack(delivery_tag, requeue=requeue))