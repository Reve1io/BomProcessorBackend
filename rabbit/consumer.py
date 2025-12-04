import os
import json
import pika
from api.NexarClient import NexarClient

class NexarWorker:
    def __init__(self):
        self.client = NexarClient(
            os.getenv("CLIENT_ID"),
            os.getenv("CLIENT_SECRET")
        )

    def process_task(self, payload):
        gql = payload["gql"]
        variables = payload["variables"]
        return self.client.get_query(gql, variables)

def callback(ch, method, props, body):
    payload = json.loads(body)

    worker = NexarWorker()
    result = worker.process_task(payload)

    ch.basic_publish(
        exchange="",
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id
        ),
        body=json.dumps(result),
    )
    ch.basic_ack(method.delivery_tag)

def main():
    params = pika.ConnectionParameters(
        host=os.getenv("BROKER_IP"),
        port=int(os.getenv("BROKER_PORT")),
        virtual_host=os.getenv("BROKER_HOST"),
        credentials=pika.PlainCredentials(
            os.getenv("BROKER_USER"),
            os.getenv("BROKER_PASSWORD")
        )
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue="Nexar_SupSearch_Tasks", durable=True)
    channel.queue_declare(queue="Nexar_SupMultiMatch_Tasks", durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="Nexar_SupSearch_Tasks", on_message_callback=callback)
    channel.basic_consume(queue="Nexar_SupMultiMatch_Tasks", on_message_callback=callback)

    print(" [x] Nexar Worker started")
    channel.start_consuming()

if __name__ == "__main__":
    main()
