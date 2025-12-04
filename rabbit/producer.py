import os
import pika
import uuid
import json
from dotenv import load_dotenv

load_dotenv()

class RabbitMQProducer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.getenv("BROKER_IP"),
                port=os.getenv("BROKER_PORT"),
                virtual_host=os.getenv("BROKER_HOST"),
                credentials=pika.PlainCredentials(os.getenv("BROKER_USER"), os.getenv("BROKER_PASSWORD")),
            )
        )
        self.channel = self.connection.channel()

        # Очередь задач
        self.channel.queue_declare(queue="Nexar_SupSearch_Tasks", durable=True)
        self.channel.queue_declare(queue="Nexar_SupMultiMatch_Tasks", durable=True)

        # Очередь ответов
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True,
        )

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body)

    def call_supsearch(self, payload):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        self.channel.basic_publish(
            exchange='',
            routing_key='Nexar_SupSearch_Tasks',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                delivery_mode=2
            ),
            body=json.dumps(payload),
        )

        while self.response is None:
            self.connection.process_data_events(time_limit=1)

        return self.response

    def call(self, payload):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        self.channel.basic_publish(
            exchange='',
            routing_key='Nexar_SupMultiMatch_Tasks',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                delivery_mode=2
            ),
            body=json.dumps(payload),
        )

        # Ждём ответа worker
        while self.response is None:
            self.connection.process_data_events(time_limit=1)

        return self.response
