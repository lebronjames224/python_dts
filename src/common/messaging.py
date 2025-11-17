import json
import pika
from pika.exceptions import AMQPConnectionError

from src.common.config import settings
from src.common.logging import get_logger

logger = get_logger(__name__)

class Rabbit:
    def __init__(self, heartbeat: int | None = None, queue_name: str | None = None):
        hb = settings.rabbitmq_heartbeat if heartbeat is None else heartbeat
        self.params = pika.URLParameters(settings.rabbitmq_url)
        self.params.heartbeat = hb
        self.params.blocked_connection_timeout = 300
        self.params.connection_attempts = 5
        self.params.retry_delay = 2
        self.queue_name = queue_name or settings.rabbitmq_queue
        self._connect()

    def _connect(self):
        try:
            self.conn = pika.BlockingConnection(self.params)
        except AMQPConnectionError as exc:
            logger.error("RabbitMQ connection failed: %s", exc)
            raise
        self.ch = self.conn.channel()
        self.ch.queue_declare(queue=self.queue_name, durable=True)
        logger.info("Connected to RabbitMQ queue=%s", self.queue_name)

    def publish(self, body: dict, retry: bool = True):
        payload = json.dumps(body).encode()
        try:
            self.ch.basic_publish(
                exchange="", routing_key=self.queue_name, body=payload,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except Exception:
            if not retry:
                raise
            # Reconnect once and retry
            logger.warning("Rabbit publish failed, retrying after reconnect")
            self._connect()
            self.ch.basic_publish(
                exchange="", routing_key=self.queue_name, body=payload,
                properties=pika.BasicProperties(delivery_mode=2),
            )

    # (workers still need these; scheduler won’t use them)
    def consume(self, on_message):
        prefetch = settings.rabbitmq_prefetch or settings.max_concurrency
        self.ch.basic_qos(prefetch_count=prefetch)
        self.ch.basic_consume(queue=self.queue_name, on_message_callback=on_message, auto_ack=False)
        self.ch.start_consuming()

    def ack_threadsafe(self, delivery_tag):
        self.conn.add_callback_threadsafe(lambda: self.ch.basic_ack(delivery_tag))

    def nack_threadsafe(self, delivery_tag, requeue=True):
        self.conn.add_callback_threadsafe(lambda: self.ch.basic_nack(delivery_tag, requeue=requeue))
